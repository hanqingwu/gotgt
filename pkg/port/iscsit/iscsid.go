/*
Copyright 2017 The GoStor Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package iscsit

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gostor/gotgt/pkg/api"
	"github.com/gostor/gotgt/pkg/config"
	"github.com/gostor/gotgt/pkg/scsi"
	"github.com/gostor/gotgt/pkg/util"
	log "github.com/sirupsen/logrus"
)

const (
	ISCSI_MAX_TSIH    = uint16(0xffff)
	ISCSI_UNSPEC_TSIH = uint16(0)
)

const (
	STATE_INIT = iota
	STATE_RUNNING
	STATE_SHUTTING_DOWN
	STATE_TERMINATE
)

var (
	EnableStats bool
)

type ISCSITargetDriver struct {
	SCSI              *scsi.SCSITargetService
	Name              string
	iSCSITargets      map[string]*ISCSITarget
	TSIHPool          map[uint16]bool
	TSIHPoolMutex     sync.Mutex
	isClientConnected bool
	enableStats       bool
	mu                *sync.RWMutex
	l                 net.Listener
	state             uint8
	OpCode            int
	TargetStats       scsi.Stats
	clusterIP         string
	workChan          chan *iscsiTask
	stopChan          chan bool
}

func init() {
	scsi.RegisterTargetDriver(iSCSIDriverName, NewISCSITargetDriver)
}

func NewISCSITargetDriver(base *scsi.SCSITargetService) (scsi.SCSITargetDriver, error) {
	driver := &ISCSITargetDriver{
		Name:         iSCSIDriverName,
		iSCSITargets: map[string]*ISCSITarget{},
		SCSI:         base,
		TSIHPool:     map[uint16]bool{0: true, 65535: true},
		mu:           &sync.RWMutex{},
	}

	driver.workChan = make(chan *iscsiTask, 64)
	driver.stopChan = make(chan bool, 1)

	if EnableStats {
		driver.enableStats = true
		driver.TargetStats.SCSIIOCount = map[int]int64{}
	}

	go driver.iscsiTaskQueueRoutine()

	return driver, nil
}

func (s *ISCSITargetDriver) AllocTSIH() uint16 {
	var i uint16
	s.TSIHPoolMutex.Lock()
	for i = uint16(0); i < ISCSI_MAX_TSIH; i++ {
		exist := s.TSIHPool[i]
		if !exist {
			s.TSIHPool[i] = true
			s.TSIHPoolMutex.Unlock()
			return i
		}
	}
	s.TSIHPoolMutex.Unlock()
	return ISCSI_UNSPEC_TSIH
}

func (s *ISCSITargetDriver) ReleaseTSIH(tsih uint16) {
	s.TSIHPoolMutex.Lock()
	delete(s.TSIHPool, tsih)
	s.TSIHPoolMutex.Unlock()
}

func (s *ISCSITargetDriver) NewTarget(tgtName string, configInfo *config.Config) error {
	if _, ok := s.iSCSITargets[tgtName]; ok {
		return fmt.Errorf("target name has been existed")
	}
	stgt, err := s.SCSI.NewSCSITarget(len(s.iSCSITargets), "iscsi", tgtName)
	if err != nil {
		return err
	}
	tgt := newISCSITarget(stgt)
	s.iSCSITargets[tgtName] = tgt
	scsiTPG := tgt.SCSITarget.TargetPortGroups[0]
	targetConfig := configInfo.ISCSITargets[tgtName]
	for tpgt, portalIDArrary := range targetConfig.TPGTs {
		tpgtNumber, _ := strconv.ParseUint(tpgt, 10, 16)
		tgt.TPGTs[uint16(tpgtNumber)] = &iSCSITPGT{uint16(tpgtNumber), make(map[string]struct{})}
		targetPortName := fmt.Sprintf("%s,t,0x%02x", tgtName, tpgtNumber)
		scsiTPG.TargetPortGroup = append(scsiTPG.TargetPortGroup, &api.SCSITargetPort{uint16(tpgtNumber), targetPortName})
		for _, portalID := range portalIDArrary {
			portal := configInfo.ISCSIPortals[portalID]
			s.AddiSCSIPortal(tgtName, uint16(tpgtNumber), portal.Portal)
		}
	}
	return nil
}

func (s *ISCSITargetDriver) SetClusterIP(ip string) {
	s.clusterIP = ip
}

func (s *ISCSITargetDriver) RereadTargetLUNMap() {
	s.SCSI.RereadTargetLUNMap()
}

func (s *ISCSITargetDriver) AddiSCSIPortal(tgtName string, tpgt uint16, portal string) error {
	var (
		ok       bool
		target   *ISCSITarget
		tpgtInfo *iSCSITPGT
	)

	if target, ok = s.iSCSITargets[tgtName]; !ok {
		return fmt.Errorf("No such target: %s", tgtName)
	}

	if tpgtInfo, ok = target.TPGTs[tpgt]; !ok {
		return fmt.Errorf("No such TPGT: %d", tpgt)
	}
	tgtPortals := tpgtInfo.Portals

	if _, ok = tgtPortals[portal]; !ok {
		tgtPortals[portal] = struct{}{}
	} else {
		return fmt.Errorf("duplicate portal %s,in %s,%d", portal, tgtName, tpgt)
	}

	return nil
}

func (s *ISCSITargetDriver) HasPortal(tgtName string, tpgt uint16, portal string) bool {
	var (
		ok       bool
		target   *ISCSITarget
		tpgtInfo *iSCSITPGT
	)

	if target, ok = s.iSCSITargets[tgtName]; !ok {
		return false
	}
	if tpgtInfo, ok = target.TPGTs[tpgt]; !ok {
		return false
	}
	tgtPortals := tpgtInfo.Portals

	if _, ok = tgtPortals[portal]; !ok {
		return false
	} else {
		return true
	}
}

func (s *ISCSITargetDriver) Run() error {
	l, err := net.Listen("tcp", ":3260")
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	s.mu.Lock()
	s.l = l
	s.mu.Unlock()
	log.Infof("iSCSI service listening on: %v", s.l.Addr())

	s.setState(STATE_RUNNING)
	for {
		conn, err := l.Accept()
		if err != nil {
			if err, ok := err.(net.Error); ok {
				if !err.Temporary() {
					log.Warning("Closing connection with initiator...")
					break
				}
			}
			log.Error(err)
			continue
		}

		log.Info(conn.LocalAddr().String())
		s.setClientStatus(true)

		//new iscsiConn
		iscsiConn := &iscsiConnection{conn: conn,
			loginParam: &iscsiLoginParam{}}

		iscsiConn.init()
		iscsiConn.rxIOState = IOSTATE_RX_BHS
		log.Infof("Target is connected to initiator: %s", conn.RemoteAddr().String())
		// start a new thread to do with this command
		go s.handler(DATAIN, iscsiConn)
	}
	return nil
}

func (s *ISCSITargetDriver) setClientStatus(ok bool) {
	s.isClientConnected = ok
}

func (s *ISCSITargetDriver) isInitiatorConnected() bool {
	return s.isClientConnected
}
func (s *ISCSITargetDriver) Close() error {
	s.mu.Lock()
	l := s.l
	s.setClientStatus(false)
	s.mu.Unlock()
	if l != nil {
		s.setState(STATE_SHUTTING_DOWN)
		if err := l.Close(); err != nil {
			return err
		}
		s.setState(STATE_TERMINATE)
		return nil
	}
	return nil
}

func (s *ISCSITargetDriver) setState(st uint8) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = st
}

func (s *ISCSITargetDriver) Resize(size uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.SCSI.Resize(size)
}

func (s *ISCSITargetDriver) handler(events byte, conn *iscsiConnection) {

	txStopChan := make(chan bool, 1)

	conn.txWorkChan = make(chan []byte, 16)
	go s.txHandlerWrite(conn, txStopChan)
	//读写分离
	for {
		err := s.rxHandler(conn)
		if err != nil {
			break
		}
	}
	/*
		if events&DATAIN != 0 {
			log.Debug("rx handler processing...")
			go s.rxHandler(conn)
		}
		if conn.state != CONN_STATE_CLOSE && events&DATAOUT != 0 {
			log.Debug("tx handler processing...")
			s.txHandler(conn)
		}
	*/
	txStopChan <- true
	close(txStopChan)
	close(conn.txWorkChan)

	//	if conn.state == CONN_STATE_CLOSE {
	log.Warningf("iscsi connection[%d] closed", conn.cid)
	//释放routine
	conn.close()
	//	}

}

func (s *ISCSITargetDriver) rxHandler(conn *iscsiConnection) error {
	var (
		hdigest uint = 0
		ddigest uint = 0
		final   bool = false
		cmd     *ISCSICommand
		buf     []byte = make([]byte, BHS_SIZE)
		length  int
		err     error
	)
	conn.readLock.Lock()
	defer conn.readLock.Unlock()
	if conn.state == CONN_STATE_SCSI {
		hdigest = conn.loginParam.sessionParam[ISCSI_PARAM_HDRDGST_EN].Value & DIGEST_CRC32C
		ddigest = conn.loginParam.sessionParam[ISCSI_PARAM_DATADGST_EN].Value & DIGEST_CRC32C
	}

	log.Debugf("rxHandler conn.rxIOState %v", conn.rxIOState)

	for {
		switch conn.rxIOState {
		case IOSTATE_RX_BHS:
			log.Debug("rx handler: IOSTATE_RX_BHS")
			length, err = conn.readData(buf)
			if err != nil {
				log.Error(err)
				return err
			}
			if length == 0 {
				log.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
				return err
			}
			cmd, err = parseHeader(buf)
			if err != nil {
				log.Error(err)
				log.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
				return err
			}
			conn.req = cmd
			if length == BHS_SIZE && cmd.DataLen != 0 {
				conn.rxIOState = IOSTATE_RX_INIT_AHS
				break
			}
			if log.GetLevel() == log.DebugLevel {
				log.Debugf("got command: \n%s", cmd.String())
				log.Debugf("got buffer: %v", buf)
			}
			final = true
		case IOSTATE_RX_INIT_AHS:
			conn.rxIOState = IOSTATE_RX_DATA
			break
			if hdigest != 0 {
				conn.rxIOState = IOSTATE_RX_INIT_HDIGEST
			}
		case IOSTATE_RX_DATA:
			if ddigest != 0 {
				conn.rxIOState = IOSTATE_RX_INIT_DDIGEST
			}
			if cmd == nil {
				return nil
			}
			dl := ((cmd.DataLen + DataPadding - 1) / DataPadding) * DataPadding
			cmd.RawData = make([]byte, int(dl))
			length := 0
			for length < dl {
				l, err := conn.readData(cmd.RawData[length:])
				if err != nil {
					log.Error(err)
					return err
				}
				length += l
			}
			if length != dl {
				log.Debugf("get length is %d, but expected %d", length, dl)
				log.Warning("set connection to close")
				conn.state = CONN_STATE_CLOSE
				errorinfo := fmt.Sprintf("get length is %d, but expected", length, dl)
				return errors.New(errorinfo)
			}
			final = true
		default:
			errorinfo := fmt.Sprintf("error %d %d\n", conn.state, conn.rxIOState)
			log.Error(errorinfo)
			return errors.New(errorinfo)
		}

		if final {
			break
		}
	}

	if conn.state == CONN_STATE_SCSI {
		log.Debugf("read buf command %x ", conn.req.TaskTag)
		s.scsiCommandHandler(conn)
	} else {
		conn.txIOState = IOSTATE_TX_BHS
		//	conn.resp = &ISCSICommand{}
		var resp *ISCSICommand
		var err error
		switch conn.req.OpCode {
		case OpLoginReq:
			log.Debug("OpLoginReq")
			resp, err = s.iscsiExecLogin(conn)
			if err != nil {
				log.Error(err)
				log.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
			}
		case OpLogoutReq:
			log.Debug("OpLogoutReq")
			s.setClientStatus(false)
			resp, err = iscsiExecLogout(conn)
			if err != nil {
				log.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
			}
		case OpTextReq:
			log.Debug("OpTextReq")
			resp, err = s.iscsiExecText(conn)
			if err != nil {
				log.Warningf("set connection to close")
				conn.state = CONN_STATE_CLOSE
			}
		default:
			resp, err = iscsiExecReject(conn)
		}
		log.Debugf("connection state is %v", conn.State())
		//需要发送的数据放给OUT线程
		s.txHandler(conn, resp)
	}

	return nil
}

func (s *ISCSITargetDriver) iscsiExecLogin(conn *iscsiConnection) (*ISCSICommand, error) {
	var cmd = conn.req

	conn.cid = cmd.ConnID
	conn.loginParam.iniCSG = cmd.CSG
	conn.loginParam.iniNSG = cmd.NSG
	conn.loginParam.iniCont = cmd.Cont
	conn.loginParam.iniTrans = cmd.Transit
	conn.loginParam.isid = cmd.ISID
	conn.loginParam.tsih = cmd.TSIH
	conn.expCmdSN = cmd.CmdSN
	conn.maxBurstLength = MaxBurstLength
	conn.maxRecvDataSegmentLength = MaxRecvDataSegmentLength
	conn.maxSeqCount = conn.maxBurstLength / conn.maxRecvDataSegmentLength

	if conn.loginParam.iniCSG == SecurityNegotiation {
		if err := conn.processSecurityData(); err != nil {
			return nil, err
		}
		conn.state = CONN_STATE_LOGIN
		return conn.buildRespPackage(OpLoginResp, nil)

	}

	if _, err := conn.processLoginData(); err != nil {
		return nil, err
	}

	if !conn.loginParam.paramInit {
		if err := s.BindISCSISession(conn); err != nil {
			conn.state = CONN_STATE_EXIT
			return nil, err
		}
		conn.loginParam.paramInit = true
		//	go s.iscsiTaskQueueRoutine(conn)
	}
	if conn.loginParam.tgtNSG == FullFeaturePhase &&
		conn.loginParam.tgtTrans {
		conn.state = CONN_STATE_LOGIN_FULL
	} else {
		conn.state = CONN_STATE_LOGIN
	}

	return conn.buildRespPackage(OpLoginResp, nil)
}

func iscsiExecLogout(conn *iscsiConnection) (*ISCSICommand, error) {
	log.Infof("Logout request received from initiator: %v", conn.conn.RemoteAddr().String())
	cmd := conn.req
	resp := &ISCSICommand{
		OpCode:  OpLogoutResp,
		StatSN:  cmd.ExpStatSN,
		TaskTag: cmd.TaskTag,
	}
	if conn.session == nil {
		resp.ExpCmdSN = cmd.CmdSN
		resp.MaxCmdSN = cmd.CmdSN
	} else {
		resp.ExpCmdSN = conn.session.ExpCmdSN
		resp.MaxCmdSN = conn.session.ExpCmdSN + conn.session.MaxQueueCommand
	}
	return resp, nil
}

func (s *ISCSITargetDriver) iscsiExecText(conn *iscsiConnection) (*ISCSICommand, error) {
	var result = []util.KeyValue{}
	cmd := conn.req
	keys := util.ParseKVText(cmd.RawData)
	if st, ok := keys["SendTargets"]; ok {
		if st == "All" {
			for name, tgt := range s.iSCSITargets {
				log.Debugf("iscsi target: %v", name)
				//log.Debugf("iscsi target portals: %v", tgt.Portals)

				result = append(result, util.KeyValue{
					Key:   "TargetName",
					Value: name,
				})
				if s.clusterIP == "" {
					for _, tpgt := range tgt.TPGTs {
						for portal := range tpgt.Portals {
							targetPort := fmt.Sprintf("%s,%d", portal, tpgt.TPGT)
							result = append(result, util.KeyValue{
								Key:   "TargetAddress",
								Value: targetPort,
							})
						}
					}
				} else {
					for _, tpgt := range tgt.TPGTs {
						targetPort := fmt.Sprintf("%s,%d", s.clusterIP, tpgt.TPGT)
						result = append(result, util.KeyValue{
							Key:   "TargetAddress",
							Value: targetPort,
						})
					}
				}
			}
		}
	}

	resp := &ISCSICommand{
		OpCode:   OpTextResp,
		Final:    true,
		NSG:      FullFeaturePhase,
		StatSN:   cmd.ExpStatSN,
		TaskTag:  cmd.TaskTag,
		ExpCmdSN: cmd.CmdSN,
		MaxCmdSN: cmd.CmdSN,
	}
	resp.RawData = util.MarshalKVText(result)
	return resp, nil
}

func iscsiExecNoopOut(conn *iscsiConnection) (*ISCSICommand, error) {
	return conn.buildRespPackage(OpNoopIn, nil)
}

func iscsiExecReject(conn *iscsiConnection) (*ISCSICommand, error) {
	return conn.buildRespPackage(OpReject, nil)
}

func iscsiExecR2T(conn *iscsiConnection, task *iscsiTask) (*ISCSICommand, error) {
	return conn.buildRespPackage(OpReady, task)
}

func (s *ISCSITargetDriver) txHandlerWrite(conn *iscsiConnection, txStopChan chan bool) {

	for {
		select {
		case buf := <-conn.txWorkChan:
			log.Debug("txHandlerWrite size ", len(buf))
			if l, err := conn.write(buf); err != nil {
				log.Errorf("failed to write data to client: %v", err)
				return
			} else {
				log.Debugf("success to write %d length", l)
			}
		case <-txStopChan:
			log.Debug("txHandlerWrite exit ")
			return
		}
	}
	//			if l, err := conn.write(resp.Bytes()); err != nil {

}

func (s *ISCSITargetDriver) txHandler(conn *iscsiConnection, resp *ISCSICommand) {
	if resp == nil {
		return
	}

	log.Debugf("enter txHander resp.Task %x ", resp.TaskTag)

	var (
		hdigest uint   = 0
		ddigest uint   = 0
		offset  uint32 = 0
		final   bool   = false
		count   uint32 = 0
	)
	if conn.state == CONN_STATE_SCSI {
		hdigest = conn.loginParam.sessionParam[ISCSI_PARAM_HDRDGST_EN].Value & DIGEST_CRC32C
		ddigest = conn.loginParam.sessionParam[ISCSI_PARAM_DATADGST_EN].Value & DIGEST_CRC32C
	}
	/*
		if conn.state == CONN_STATE_SCSI && conn.txTask == nil {
			err := s.scsiCommandHandler(conn)
			if err != nil {
				log.Error(err)
				return
			}
		}
	*/
	//	resp := conn.resp
	segmentLen := conn.maxRecvDataSegmentLength
	transferLen := len(resp.RawData)
	resp.DataSN = 0
	maxCount := conn.maxSeqCount
	txIOState := IOSTATE_TX_BHS

	if s.enableStats {
		if resp.OpCode == OpSCSIResp || resp.OpCode == OpSCSIIn {
			s.UpdateStats(conn, resp)
		}
	}

	/* send data splitted by segmentLen */
SendRemainingData:
	log.Debugf("transferLen %v, offset %v , segmentLen %v ", transferLen, offset, segmentLen)
	if resp.OpCode == OpSCSIIn {
		resp.BufferOffset = offset
		if int(offset+segmentLen) < transferLen {
			count += 1
			if count < maxCount {
				resp.FinalInSeq = false
				resp.Final = false
			} else {
				count = 0
				resp.FinalInSeq = true
				resp.Final = false
			}
			offset = offset + segmentLen
			resp.DataLen = int(segmentLen)
		} else {
			resp.FinalInSeq = true
			resp.Final = true
			resp.DataLen = transferLen - int(offset)
		}
	}
	for {
		switch txIOState {
		case IOSTATE_TX_BHS:
			if log.GetLevel() == log.DebugLevel {
				log.Debug("ready to write response")
				log.Debugf("response is %s", resp.String())
			}

			conn.txWorkChan <- resp.Bytes()
			txIOState = IOSTATE_TX_INIT_AHS
		/*
			if l, err := conn.write(resp.Bytes()); err != nil {
				log.Errorf("failed to write data to client: %v", err)
				return
			} else {
				conn.txIOState = IOSTATE_TX_INIT_AHS
				log.Debugf("success to write %d length", l)
			}
		*/
		case IOSTATE_TX_INIT_AHS:
			if hdigest != 0 {
				txIOState = IOSTATE_TX_INIT_HDIGEST
			} else {
				txIOState = IOSTATE_TX_INIT_DATA
			}
			if txIOState != IOSTATE_TX_AHS {
				final = true
			}
		case IOSTATE_TX_AHS:
		case IOSTATE_TX_INIT_DATA:
			final = true
		case IOSTATE_TX_DATA:
			if ddigest != 0 {
				txIOState = IOSTATE_TX_INIT_DDIGEST
			}
		default:
			log.Errorf("error %d %d\n", conn.state, txIOState)
			return
		}

		if final {
			if resp.OpCode == OpSCSIIn && resp.Final != true {
				resp.DataSN++
				txIOState = IOSTATE_TX_BHS
				goto SendRemainingData
			} else {
				break
			}
		}
	}

	log.Debugf("connection state: %v", conn.State())
	switch conn.state {
	case CONN_STATE_CLOSE, CONN_STATE_EXIT:
		conn.state = CONN_STATE_CLOSE
	case CONN_STATE_SECURITY_LOGIN:
		conn.state = CONN_STATE_LOGIN
	case CONN_STATE_LOGIN:
		conn.rxIOState = IOSTATE_RX_BHS
	//	s.handler(DATAIN, conn)
	case CONN_STATE_SECURITY_FULL, CONN_STATE_LOGIN_FULL:
		if conn.session.SessionType == SESSION_NORMAL {
			conn.state = CONN_STATE_KERNEL
			conn.state = CONN_STATE_SCSI
		} else {
			conn.state = CONN_STATE_FULL
		}
		conn.rxIOState = IOSTATE_RX_BHS
	//	s.handler(DATAIN, conn)
	case CONN_STATE_SCSI:
		conn.txTask = nil
	default:
		log.Warnf("unexpected connection state: %v", conn.State())
		conn.rxIOState = IOSTATE_RX_BHS
		//		s.handler(DATAIN, conn)
	}
}

func (s *ISCSITargetDriver) scsiCommandHandler(conn *iscsiConnection) (err error) {
	var resp *ISCSICommand

	req := conn.req
	switch req.OpCode {
	case OpSCSICmd:
		log.Debugf("SCSI Command tag %x  processing...", req.TaskTag)
		scmd := &api.SCSICommand{
			ITNexusID:       conn.session.ITNexus.ID,
			SCB:             req.CDB,
			SCBLength:       len(req.CDB),
			Lun:             req.LUN,
			Tag:             uint64(req.TaskTag),
			RelTargetPortID: conn.session.TPGT,
		}
		if req.Read {
			if req.Write {
				scmd.Direction = api.SCSIDataBidirection
			} else {
				scmd.Direction = api.SCSIDataRead
			}
		} else {
			if req.Write {
				scmd.Direction = api.SCSIDataWrite
			}
		}

		task := &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag, scmd: scmd}
		task.scmd.OpCode = conn.req.SCSIOpCode
		if scmd.Direction == api.SCSIDataBidirection {
			task.scmd.Result = api.SAMStatCheckCondition.Stat
			scsi.BuildSenseData(task.scmd, scsi.ILLEGAL_REQUEST, scsi.NO_ADDITIONAL_SENSE)
			conn.buildRespPackage(OpSCSIResp, task)
			conn.rxTask = nil
			break
		}
		if req.Write {
			task.r2tCount = int(req.ExpectedDataLen) - req.DataLen
			task.expectedDataLength = int64(req.ExpectedDataLen)
			if !req.Final {
				task.unsolCount = 1
			}
			// new buffer for the data out
			if scmd.OutSDBBuffer == nil {
				blen := int(req.ExpectedDataLen)
				if blen == 0 {
					blen = int(req.DataLen)
				}
				scmd.OutSDBBuffer = &api.SCSIDataBuffer{
					Length: uint32(blen),
					Buffer: make([]byte, blen),
				}
			}
			log.Debugf("SCSI write, R2T count: %d, unsol Count: %d, offset: %d", task.r2tCount, task.unsolCount, task.offset)

			if conn.session.SessionParam[ISCSI_PARAM_IMM_DATA_EN].Value == 1 {
				copy(scmd.OutSDBBuffer.Buffer[task.offset:], conn.req.RawData)
				task.offset += conn.req.DataLen
			}
			if task.r2tCount > 0 {
				// prepare to receive more data
				conn.session.ExpCmdSN += 1
				task.state = taskPending
				conn.session.PendingTasksMutex.Lock()
				conn.session.PendingTasks.Push(task)
				conn.session.PendingTasksMutex.Unlock()
				conn.rxTask = task
				if conn.session.SessionParam[ISCSI_PARAM_INITIAL_R2T_EN].Value == 1 {
					resp, err := iscsiExecR2T(conn, task)
					if err != nil {
						log.Error("iscsiExeR2T error")
					} else {
						s.txHandler(conn, resp)
					}
					break
				} else {
					log.Debugf("Not ready to exec the task")
					conn.rxIOState = IOSTATE_RX_BHS
					//		s.handler(DATAIN, conn)
					return nil
				}
			}
		} else if scmd.InSDBBuffer == nil {
			scmd.InSDBBuffer = &api.SCSIDataBuffer{
				Length: uint32(req.ExpectedDataLen),
				Buffer: make([]byte, int(req.ExpectedDataLen)),
			}
		}
		task.offset = 0
		//conn.rxTask = task

		s.workChan <- task
		log.Debugf("send workchan task.Tag %x ", task.tag)

		conn.rxIOState = IOSTATE_RX_BHS
		//		s.handler(DATAIN, conn)
		return
	case OpSCSITaskReq:
		// task management function
		task := &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag, scmd: nil}
		conn.rxTask = task
		if err := s.iscsiTaskQueueHandler(task); err != nil {
			return err
		}
	case OpSCSIOut:
		log.Debugf("iSCSI Data-out processing...")
		conn.session.PendingTasksMutex.RLock()
		task := conn.session.PendingTasks.GetByTag(conn.req.TaskTag)
		conn.session.PendingTasksMutex.RUnlock()
		if task == nil {
			err = fmt.Errorf("Cannot find iSCSI task with tag[%v]", conn.req.TaskTag)
			log.Error(err)
			return err
		}
		copy(task.scmd.OutSDBBuffer.Buffer[task.offset:], conn.req.RawData)
		task.offset += conn.req.DataLen
		task.r2tCount = task.r2tCount - conn.req.DataLen
		log.Debugf("Final: %v", conn.req.Final)
		log.Debugf("r2tCount: %v", task.r2tCount)
		if !conn.req.Final {
			log.Debugf("Not ready to exec the task")
			conn.rxIOState = IOSTATE_RX_BHS
			//			s.handler(DATAIN, conn)
			return nil
		} else if task.r2tCount > 0 {
			// prepare to receive more data
			if task.unsolCount == 0 {
				task.r2tSN += 1
			} else {
				task.r2tSN = 0
				task.unsolCount = 0
			}
			conn.rxTask = task
			resp, err = iscsiExecR2T(conn, task)
			break
		}
		task.offset = 0
		log.Debugf("Process the Data-out package")
		conn.rxTask = task
		resp, err = s.iscsiExecTask(task)
		if err != nil {
			return
		} else {
			resp, err = conn.buildRespPackage(OpSCSIResp, task)
			conn.rxTask = nil
			conn.session.PendingTasksMutex.Lock()
			conn.session.PendingTasks.RemoveByTag(conn.req.TaskTag)
			conn.session.PendingTasksMutex.Unlock()
		}
	case OpNoopOut:
		resp, err = iscsiExecNoopOut(conn)
	case OpLogoutReq:
		s.setClientStatus(false)
		conn.txTask = &iscsiTask{conn: conn, cmd: conn.req, tag: conn.req.TaskTag}
		conn.txIOState = IOSTATE_TX_BHS
		resp, err = iscsiExecLogout(conn)
	case OpTextReq, OpSNACKReq:
		err = fmt.Errorf("Cannot handle yet %s", opCodeMap[conn.req.OpCode])
		log.Error(err)
		return
	default:
		err = fmt.Errorf("Unknown op %s", opCodeMap[conn.req.OpCode])
		log.Error(err)
		return
	}
	conn.rxIOState = IOSTATE_RX_BHS
	s.txHandler(conn, resp)
	return nil
}

func (s *ISCSITargetDriver) reactorLaunch() {
	var wg sync.WaitGroup

	log.Debug("WriterWorker  create")

	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for {
				select {
				case task := <-s.workChan:
					var resp *ISCSICommand
					var err error

					log.Debugf("go routine work task CmdSN %d, StatSN %d ", task.cmd.CmdSN, task.cmd.StatSN)
					err = s.iscsiTaskQueueHandler(task)
					if err != nil {
						return
					}

					scmd := task.scmd
					if scmd.Direction == api.SCSIDataRead && scmd.SenseBuffer == nil && task.cmd.ExpectedDataLen != 0 {
						//				if scmd.Direction == api.SCSIDataRead && scmd.SenseBuffer == nil && req.ExpectedDataLen != 0 {
						resp, err = task.conn.buildRespPackage(OpSCSIIn, task)
					} else {
						resp, err = task.conn.buildRespPackage(OpSCSIResp, task)
					}

					s.txHandler(task.conn, resp)
					continue
				case <-s.stopChan:
					log.Debug("writerwork exit")
					return

				}
			}

		}(i)
	}

	wg.Wait()
}

func (s *ISCSITargetDriver) iscsiTaskQueueRoutine() {
	s.reactorLaunch()
}

func (s *ISCSITargetDriver) iscsiTaskQueueHandler(task *iscsiTask) error {
	conn := task.conn
	sess := conn.session
	cmd := task.cmd
	if cmd.Immediate {
		_, err := s.iscsiExecTask(task)
		return err
	}
	cmdsn := cmd.CmdSN
	log.Debugf("CmdSN of command is %d", cmdsn)
	/*
		if cmdsn >= sess.ExpCmdSN {
	*/
	//	retry:
	cmdsn += 1
	sess.ExpCmdSN = cmdsn
	log.Debugf("session's ExpCmdSN is %d", cmdsn)

	log.Debugf("process task(%d)", task.cmd.CmdSN)
	if _, err := s.iscsiExecTask(task); err != nil {
		log.Error(err)
	}
	/*
		sess.PendingTasksMutex.Lock()
		if sess.PendingTasks.Len() == 0 {
			sess.PendingTasksMutex.Unlock()
			return nil
		}
		task = sess.PendingTasks.Pop()
		cmd = task.cmd
		if cmd.CmdSN != cmdsn {
			sess.PendingTasks.Push(task)
			sess.PendingTasksMutex.Unlock()
			return nil
		}
		task.state = taskSCSI
		sess.PendingTasksMutex.Unlock()
				//goto retry
			} else {
				if cmd.CmdSN < sess.ExpCmdSN {
					err := fmt.Errorf("unexpected cmd serial number: (%d, %d)", cmd.CmdSN, sess.ExpCmdSN)
					log.Error(err)
					return err
				}
				log.Debugf("add task(%d) into task queue", task.cmd.CmdSN)
				// add this task into queue and set it as a pending task
				sess.PendingTasksMutex.Lock()
				task.state = taskPending
				sess.PendingTasks.Push(task)
				sess.PendingTasksMutex.Unlock()
				return fmt.Errorf("pending")
			}
	*/
	return nil
}

func (s *ISCSITargetDriver) iscsiExecTask(task *iscsiTask) (*ISCSICommand, error) {
	cmd := task.cmd
	switch cmd.OpCode {
	case OpSCSICmd, OpSCSIOut:
		task.state = taskSCSI
		// add scsi target process queue
		err := s.SCSI.AddCommandQueue(task.conn.session.Target.SCSITarget.TID, task.scmd)
		if err != nil {
			task.state = 0
		}
		return nil, err
	case OpLogoutReq:

	case OpNoopOut:
		// just do it in iscsi layer
	case OpSCSITaskReq:
		sess := task.conn.session
		switch cmd.TaskFunc {
		case ISCSI_TM_FUNC_ABORT_TASK:
			sess.PendingTasksMutex.Lock()
			stask := sess.PendingTasks.RemoveByTag(cmd.ReferencedTaskTag)
			sess.PendingTasksMutex.Unlock()
			if stask == nil {
				task.result = ISCSI_TMF_RSP_NO_TASK
			} else {
				// abort this task
				log.Debugf("abort the task[%v]", stask.tag)
				if stask.scmd == nil {
					stask.scmd = &api.SCSICommand{Result: api.SAM_STAT_TASK_ABORTED}
				}
				stask.conn = task.conn
				log.Debugf("stask.conn: %#v", stask.conn)
				stask.conn.buildRespPackage(OpSCSIResp, stask)
				stask.conn.rxTask = nil
				//	s.handler(DATAOUT, stask.conn)
				task.result = ISCSI_TMF_RSP_COMPLETE
			}
		case ISCSI_TM_FUNC_ABORT_TASK_SET:
		case ISCSI_TM_FUNC_LOGICAL_UNIT_RESET:
		case ISCSI_TM_FUNC_CLEAR_ACA:
			fallthrough
		case ISCSI_TM_FUNC_CLEAR_TASK_SET:
			fallthrough
		case ISCSI_TM_FUNC_TARGET_WARM_RESET, ISCSI_TM_FUNC_TARGET_COLD_RESET, ISCSI_TM_FUNC_TASK_REASSIGN:
			task.result = ISCSI_TMF_RSP_NOT_SUPPORTED
		default:
			task.result = ISCSI_TMF_RSP_REJECTED
		}
		// return response to initiator
		return task.conn.buildRespPackage(OpSCSITaskResp, task)
	}
	return nil, nil
}

func (s *ISCSITargetDriver) Stats() scsi.Stats {
	s.mu.RLock()
	stats := s.TargetStats
	stats.SCSIIOCount = map[int]int64{}
	for key, value := range s.TargetStats.SCSIIOCount {
		stats.SCSIIOCount[key] = value
	}
	s.mu.RUnlock()
	return stats
}

func (s *ISCSITargetDriver) UpdateStats(conn *iscsiConnection, resp *ISCSICommand) {
	s.mu.Lock()
	s.TargetStats.IsClientConnected = s.isClientConnected
	switch api.SCSICommandType(resp.SCSIOpCode) {
	case api.READ_6, api.READ_10, api.READ_12, api.READ_16:
		s.TargetStats.ReadIOPS += 1
		s.TargetStats.TotalReadTime += int64(time.Since(resp.StartTime))
		s.TargetStats.TotalReadBlockCount += int64(resp.ExpectedDataLen)
		break
	case api.WRITE_6, api.WRITE_10, api.WRITE_12, api.WRITE_16:
		s.TargetStats.WriteIOPS += 1
		s.TargetStats.TotalWriteTime += int64(time.Since(resp.StartTime))
		s.TargetStats.TotalWriteBlockCount += int64(resp.ExpectedDataLen)
		break
	}
	s.TargetStats.SCSIIOCount[(int)(resp.SCSIOpCode)] += 1
	s.mu.Unlock()
}

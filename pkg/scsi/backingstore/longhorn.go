/*
Copyright 2017 The GoStor Authors All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this Longhorn except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backingstore

import (
	"fmt"

	"github.com/gostor/gotgt/pkg/api"
	"github.com/gostor/gotgt/pkg/scsi"
	//	"github.com/gostor/gotgt/pkg/util"
	log "github.com/sirupsen/logrus"
)

const (
	LonghornBackingStorage = "longhorn"
)

func init() {
	scsi.RegisterBackingStore(LonghornBackingStorage, newLonghorn)
}

type LonghornBackingStore struct {
	scsi.BaseBackingStore
	RemBs api.RemoteBackingStore
}

func newLonghorn() (api.BackingStore, error) {
	return &LonghornBackingStore{
		BaseBackingStore: scsi.BaseBackingStore{
			Name:            LonghornBackingStorage,
			DataSize:        0,
			OflagsSupported: 0,
		},
	}, nil
}

func (bs *LonghornBackingStore) Open(dev *api.SCSILu, path string) error {
	log.Info("longhorn gotgt open")

	var err error
	bs.DataSize = 0 //uint64(finfo.Size())
	bs.RemBs, err = scsi.GetTargetBSMap(path)
	if err != nil {
		log.Info("LonghornBackingStore Open get  Rembs err: ", err)
		return err
	}

	return nil
}

func (bs *LonghornBackingStore) Close(dev *api.SCSILu) error {
	log.Info("longhorn gotgt close")
	return nil
}

func (bs *LonghornBackingStore) Init(dev *api.SCSILu, Opts string) error {
	return nil
}

func (bs *LonghornBackingStore) Exit(dev *api.SCSILu) error {
	return nil
}

func (bs *LonghornBackingStore) Size(dev *api.SCSILu) uint64 {
	return bs.DataSize
}

func (bs *LonghornBackingStore) Read(offset, tl int64) ([]byte, error) {
	tmpbuf := make([]byte, tl)
	length, err := bs.RemBs.ReadAt(tmpbuf, offset)
	if err != nil {
		return nil, err
	}
	if length != len(tmpbuf) {
		return nil, fmt.Errorf("read is not same length of length")
	}
	return tmpbuf, nil
}

func (bs *LonghornBackingStore) Write(wbuf []byte, offset int64) error {
	length, err := bs.RemBs.WriteAt(wbuf, offset)
	if err != nil {
		log.Error(err)
		return err
	}
	if length != len(wbuf) {
		return fmt.Errorf("write is not same length of length")
	}
	return nil
}

func (bs *LonghornBackingStore) DataSync(offset, tl int64) error {
	return nil
}

func (bs *LonghornBackingStore) DataAdvise(offset, length int64, advise uint32) error {
	return nil
}

func (bs *LonghornBackingStore) Unmap([]api.UnmapBlockDescriptor) error {
	return nil
}

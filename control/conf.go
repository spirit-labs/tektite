package control

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
	"time"
)

type Conf struct {
	ControllerStateUpdaterBucketName string
	ControllerStateUpdaterKeyPrefix  string
	ControllerMetaDataBucketName     string
	ControllerMetaDataKeyPrefix      string
	SSTableBucketName                string
	DataFormat                       common.DataFormat
	TableNotificationInterval        time.Duration
	LsmConf                          lsm.Conf
	SequencesBlockSize               int
	AzInfo                           string
	LsmStateWriteInterval            time.Duration
}

func NewConf() Conf {
	return Conf{
		ControllerStateUpdaterBucketName: "controller-state-updater",
		ControllerStateUpdaterKeyPrefix:  "controller-state-updater-key",
		ControllerMetaDataBucketName:     "controller-meta-data",
		ControllerMetaDataKeyPrefix:      "controller-meta-data",
		SSTableBucketName:                "tektite-data",
		DataFormat:                       common.DataFormatV1,
		TableNotificationInterval:        5 * time.Second,
		LsmConf:                          lsm.NewConf(),
		SequencesBlockSize:               100,
		LsmStateWriteInterval:            10 * time.Millisecond,
	}
}

func (c *Conf) Validate() error {
	if err := c.LsmConf.Validate(); err != nil {
		return err
	}
	return nil
}

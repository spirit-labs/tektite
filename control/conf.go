package control

import (
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/lsm"
)

type Conf struct {
	ControllerStateUpdaterBucketName string
	ControllerStateUpdaterKeyPrefix  string
	ControllerMetaDataBucketName     string
	ControllerMetaDataKeyPrefix      string
	SSTableBucketName                string
	DataFormat common.DataFormat
	LsmConf                          lsm.Conf
}

func NewConf() Conf {
	return Conf{
		ControllerStateUpdaterBucketName: "controller-state-updater",
		ControllerStateUpdaterKeyPrefix:  "controller-state-updater-key",
		ControllerMetaDataBucketName:     "controller-meta-data",
		ControllerMetaDataKeyPrefix:      "controller-meta-data",
		SSTableBucketName:                "tektite-data",
		DataFormat: common.DataFormatV1,
		LsmConf:                          lsm.NewConf(),
	}
}

func (c *Conf) Validate() error {
	if err := c.LsmConf.Validate(); err != nil {
		return err
	}
	return nil
}

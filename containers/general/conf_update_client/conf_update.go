package confupdateclient

import (
	"errors"
	"fmt"
	"time"

	"github.com/refractionPOINT/go-limacharlie/limacharlie"
)

type ConfUpdateClient struct {
	oid      string
	confGUID string

	lc     *limacharlie.Organization
	h      *limacharlie.HiveClient
	logger limacharlie.LCLogger

	isRunning bool
	lastEtag  string
}

func NewConfUpdateClient(oid string, confGUID string, logger limacharlie.LCLogger) (*ConfUpdateClient, map[string]interface{}, error) {
	lc, err := limacharlie.NewOrganizationFromClientOptions(limacharlie.ClientOptions{
		OID: oid,
	}, logger)
	if err != nil {
		return nil, nil, err
	}
	h := limacharlie.NewHiveClient(lc)

	// The call to new returns the first instance of the config or an
	// error from trying to get it.
	// Make a hive call to
	rec, err := h.GetPublicByGUID(limacharlie.HiveArgs{
		HiveName:     "external_adapter",
		PartitionKey: oid,
	}, confGUID)
	if err != nil {
		return nil, nil, err
	}

	return &ConfUpdateClient{
		oid:       oid,
		confGUID:  confGUID,
		lc:        lc,
		h:         h,
		logger:    logger,
		isRunning: false,
		lastEtag:  rec.SysMtd.Etag,
	}, rec.Data, nil
}

func (c *ConfUpdateClient) Close() error {
	c.isRunning = false
	c.lc.Close()
	return nil
}

func (c *ConfUpdateClient) WatchForChanges(period time.Duration, cb func(data map[string]interface{})) error {
	if c.isRunning {
		return errors.New("already running")
	}
	c.isRunning = true

	for {
		time.Sleep(period)
		if !c.isRunning {
			return nil
		}
		rec, err := c.h.GetPublicByGUID(limacharlie.HiveArgs{
			HiveName:     "external_adapter",
			PartitionKey: c.oid,
			ETag:         &c.lastEtag,
		}, c.confGUID)
		if err != nil {
			c.logger.Error(fmt.Sprintf("error getting public record: %v", err))
			continue
		}
		if len(rec.Data) == 0 {
			// No data means no change to the record.
			continue
		}
		c.lastEtag = rec.SysMtd.Etag
		c.logger.Info("conf update received")
		cb(rec.Data)
	}
}

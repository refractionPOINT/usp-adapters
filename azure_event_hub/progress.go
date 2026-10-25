package usp_azure_event_hub

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Azure/azure-amqp-common-go/v4/conn"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
)

const (
	maxPartitions    = 4096
	maxIdentityBytes = 1024
)

type partitionPosition struct {
	Offset   string
	Sequence int64
	Enqueued int64 // Unix nanoseconds; never an activity-time replay boundary
}

// The source contains no credentials. A key/token rotation must keep the same
// namespace, entity and existing default consumer group fingerprint.
func sourceFingerprint(connection string) ([32]byte, error) {
	// Use the SAME parser as Hub: do not normalize an entity differently from
	// the actual receiver (for example trimming a significant trailing space).
	parsed, err := conn.ParsedConnectionFromStr(connection)
	if err != nil {
		return [32]byte{}, err
	}
	if parsed.HubName == "" {
		return [32]byte{}, errors.New(`key "EntityPath" must not be empty`)
	}
	identity := strings.ToLower(parsed.Namespace + "." + parsed.Suffix)
	return sha256.Sum256([]byte("event-hubs\x00" + identity + "\x00" + parsed.HubName + "\x00" + eventhub.DefaultConsumerGroup)), nil
}

func validPartition(id string) bool {
	return id != "" && len(id) <= maxIdentityBytes && utf8.ValidString(id) && strings.IndexFunc(id, unicode.IsControl) < 0
}
func validPosition(p partitionPosition) bool {
	// Numeric offsets are interpolated into the SDK's exclusive selector. Never
	// accept arbitrary selector syntax. Preserve any leading zeros exactly.
	if p.Offset == "" || len(p.Offset) > 19 || p.Sequence < 0 || p.Enqueued <= 0 {
		return false
	}
	for _, ch := range p.Offset {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	_, err := strconv.ParseInt(p.Offset, 10, 64)
	return err == nil
}

func (a *EventHubAdapter) issue(code string) {
	if a.conf.ClientOptions.DebugLog != nil {
		a.conf.ClientOptions.DebugLog("event hub collection: " + code)
	}
}
func (a *EventHubAdapter) confirm(g *generation) error {
	positions, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	for id, raw := range positions {
		tag := g.id
		if current, ok := g.tags[id]; ok {
			tag = current
		}
		if len(raw) != 16 || binary.BigEndian.Uint64(raw) != tag {
			continue
		}
		g.confirmed[id] = int64(binary.BigEndian.Uint64(raw[8:]))
	}
	return nil
}
func ackInterval() time.Duration {
	var b [1]byte
	_, _ = rand.Read(b[:])
	return 30*time.Second + time.Duration(b[0])*3*time.Second/256
}

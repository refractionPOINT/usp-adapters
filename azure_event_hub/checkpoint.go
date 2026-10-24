package usp_azure_event_hub

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/Azure/azure-amqp-common-go/v4/conn"
	eventhub "github.com/Azure/azure-event-hubs-go/v3"
	"github.com/refractionPOINT/usp-adapters/utils"
)

const (
	checkpointSchema        = 1
	checkpointPositionLimit = 32768 - 64 // complete store envelope has a separate 64B reserve
	maxPartitions           = 4096
	maxIdentityBytes        = 1024
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

// A canonical numeric partition takes 2B, offset/sequence/enqueue take 24B:
// 1,024 partitions occupy 26,627B. Escaped names and noncanonical numeric
// offsets round-trip exactly. Oversize snapshots fail whole, never dropping a
// partition or shortening an offset to fit.
func encodePosition(positions map[string]partitionPosition) ([]byte, error) {
	if len(positions) > maxPartitions {
		return nil, utils.ErrCheckpointLimit
	}
	keys := make([]string, 0, len(positions))
	for id, p := range positions {
		if !validPartition(id) || !validPosition(p) {
			return nil, utils.ErrCheckpointInvalid
		}
		keys = append(keys, id)
	}
	sort.Strings(keys)
	b := make([]byte, 0, 3+26*len(keys))
	b = append(b, 1)
	b = binary.BigEndian.AppendUint16(b, uint16(len(keys)))
	for _, id := range keys {
		n, err := strconv.ParseUint(id, 10, 16)
		if err == nil && n < 65535 && strconv.FormatUint(n, 10) == id {
			b = binary.BigEndian.AppendUint16(b, uint16(n))
		} else {
			b = binary.BigEndian.AppendUint16(b, 65535)
			b = binary.BigEndian.AppendUint16(b, uint16(len(id)))
			b = append(b, id...)
		}
		p := positions[id]
		offset, _ := strconv.ParseInt(p.Offset, 10, 64)
		if strconv.FormatInt(offset, 10) == p.Offset {
			b = binary.BigEndian.AppendUint64(b, uint64(offset))
		} else {
			b = binary.BigEndian.AppendUint64(b, ^uint64(0))
			b = binary.BigEndian.AppendUint16(b, uint16(len(p.Offset)))
			b = append(b, p.Offset...)
		}
		b = binary.BigEndian.AppendUint64(b, uint64(p.Sequence))
		b = binary.BigEndian.AppendUint64(b, uint64(p.Enqueued))
		if len(b) > checkpointPositionLimit {
			return nil, utils.ErrCheckpointLimit
		}
	}
	return b, nil
}

func decodePosition(b []byte) (map[string]partitionPosition, error) {
	if len(b) < 3 || len(b) > checkpointPositionLimit || b[0] != 1 {
		return nil, utils.ErrCheckpointInvalid
	}
	count := int(binary.BigEndian.Uint16(b[1:3]))
	b = b[3:]
	if count > maxPartitions {
		return nil, utils.ErrCheckpointInvalid
	}
	positions := make(map[string]partitionPosition, count)
	readString := func() (string, bool) {
		if len(b) < 2 {
			return "", false
		}
		n := int(binary.BigEndian.Uint16(b))
		b = b[2:]
		if n > len(b) {
			return "", false
		}
		s := string(b[:n])
		b = b[n:]
		return s, true
	}
	for i := 0; i < count; i++ {
		if len(b) < 2 {
			return nil, utils.ErrCheckpointInvalid
		}
		idn := binary.BigEndian.Uint16(b)
		b = b[2:]
		id := strconv.Itoa(int(idn))
		if idn == 65535 {
			var ok bool
			if id, ok = readString(); !ok {
				return nil, utils.ErrCheckpointInvalid
			}
		}
		if len(b) < 8 {
			return nil, utils.ErrCheckpointInvalid
		}
		offsetn := binary.BigEndian.Uint64(b)
		b = b[8:]
		offset := strconv.FormatUint(offsetn, 10)
		if offsetn == ^uint64(0) {
			var ok bool
			if offset, ok = readString(); !ok {
				return nil, utils.ErrCheckpointInvalid
			}
		}
		if len(b) < 16 {
			return nil, utils.ErrCheckpointInvalid
		}
		p := partitionPosition{offset, int64(binary.BigEndian.Uint64(b)), int64(binary.BigEndian.Uint64(b[8:]))}
		b = b[16:]
		if !validPartition(id) || !validPosition(p) {
			return nil, utils.ErrCheckpointInvalid
		}
		if _, exists := positions[id]; exists {
			return nil, utils.ErrCheckpointInvalid
		}
		positions[id] = p
	}
	if len(b) != 0 {
		return nil, utils.ErrCheckpointInvalid
	}
	return positions, nil
}

// All store operations, including OpenCheckpoint's ambiguous-write readback,
// receive an operation deadline as well as the adapter's cancellation.
type boundedStore struct{ utils.CheckpointStore }

func (s boundedStore) Load(ctx context.Context) (utils.Checkpoint, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.CheckpointStore.Load(ctx)
}
func (s boundedStore) CompareAndSwap(ctx context.Context, f utils.CheckpointFence, c utils.Checkpoint) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.CheckpointStore.CompareAndSwap(ctx, f, c)
}

func (a *EventHubAdapter) issue(code string) {
	if a.conf.CheckpointIssue != nil {
		a.conf.CheckpointIssue(code)
	}
}

func (a *EventHubAdapter) saveConfirmed(ctx context.Context, g *generation) error {
	confirmed, err := a.tracker.Confirm()
	if err != nil {
		a.issue("ack_observation_failed")
		return err
	}
	for id, raw := range confirmed {
		tag := g.id
		if current, ok := g.tags[id]; ok {
			tag = current
		}
		if len(raw) < 8 || binary.BigEndian.Uint64(raw) != tag {
			continue
		}
		positions, e := decodePosition(raw[8:])
		if e != nil {
			return e
		}
		if p, ok := positions[id]; ok {
			g.confirmed[id] = p
		}
	}
	raw, err := encodePosition(g.confirmed)
	if err != nil {
		// Keep the last COMPLETE checkpoint and the complete bounded live map.
		// Ingestion continues, but adoption for this partition set is limited.
		a.issue("position_size_stall")
		return nil
	}
	g.latest = raw // retained even if an older unknown Save attempt is frozen
	return a.savePosition(ctx, g)
}

// The worker owns the session until it joins; Close owns it afterwards. Once a
// save fence is lost, this acquisition never writes or claims again. Collection,
// pending ACK dependencies and the newest confirmed frontier remain intact.
func (a *EventHubAdapter) savePosition(ctx context.Context, g *generation) error {
	if g.session == nil {
		return nil
	}
	err := g.session.Save(ctx, g.latest)
	if errors.Is(err, utils.ErrCheckpointConflict) {
		g.session = nil
		g.persistenceLost = true
		a.issue("ownership_conflict")
		return nil
	}
	if err != nil {
		a.issue("save_retry")
	}
	return err
}

func checkpointSaveInterval() time.Duration {
	var entropy [1]byte
	_, _ = rand.Read(entropy[:])
	return 30*time.Second + time.Duration(entropy[0])*3*time.Second/256
}

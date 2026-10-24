package utils

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

var (
	// ErrCheckpointAllocationRefused permits release-one collection without
	// adopting a checkpoint ONLY when allocation of a missing slot is refused.
	// Ordinary transport, load, size and permission errors must not wrap it.
	ErrCheckpointAllocationRefused = errors.New("checkpoint allocation refused")
	ErrCheckpointConflict          = errors.New("checkpoint ownership changed")
	ErrCheckpointInvalid           = errors.New("checkpoint state invalid")
)

// CheckpointSession serializes one writer's store operations. Its caller must
// stop/join source traversal before discarding a conflicting session and opening
// another. It must retain the newest COMPLETE ACK-confirmed position until a
// successful Save; Confirm consumes positions even when the store is unavailable.
// No method logs store errors or calls a vendor error callback.
type CheckpointSession struct {
	store   CheckpointStore
	current Checkpoint
	pending *Checkpoint
}

// OpenCheckpoint loads and claims restart state. Call from a cancellable worker
// after the adapter constructor returns. initial is evaluated only after a
// positive missing read or a different source generation, once per such episode;
// unavailable/corrupt same-source state never selects a new start time.
// A nil session with a nonnil position means first allocation was refused.
func OpenCheckpoint(ctx context.Context, store CheckpointStore, schema uint32, source [32]byte, initial func() ([]byte, error), validate func([]byte) error, issue func(string)) (*CheckpointSession, []byte, error) {
	var first []byte
	var haveFirst bool
	delay := time.Second
	for ctx.Err() == nil {
		value, found, err := store.Load(ctx)
		if err == nil {
			fresh := !found || value.SourceFingerprint != source
			if fresh {
				if !haveFirst {
					first, err = initial()
					haveFirst = err == nil
				}
				value.Position = append([]byte(nil), first...)
			} else if value.SchemaVersion != schema || validate(value.Position) != nil {
				err = ErrCheckpointInvalid
			}
			if err == nil {
				expected := value.CheckpointFence
				if !found {
					expected = CheckpointFence{}
				}
				nonce, e := checkpointToken()
				if e != nil {
					return nil, nil, e
				}
				value.SchemaVersion, value.SourceFingerprint = schema, source
				value.Nonce = nonce
				s := &CheckpointSession{store: store, current: Checkpoint{CheckpointFence: expected}}
				value.Revision, e = checkpointToken()
				if e != nil {
					return nil, nil, e
				}
				value.Attempt, e = checkpointToken()
				if e != nil {
					return nil, nil, e
				}
				s.pending = &value
				// Retry precisely this frozen claim on an unknown write outcome.
				for ctx.Err() == nil {
					err = s.commit(ctx)
					if err == nil {
						return s, append([]byte(nil), value.Position...), nil
					}
					if errors.Is(err, ErrCheckpointAllocationRefused) && !found {
						checkpointIssue(issue, "allocation_refused")
						return nil, append([]byte(nil), value.Position...), nil
					}
					if errors.Is(err, ErrCheckpointConflict) {
						break
					}
					checkpointIssue(issue, "claim_wait")
					if !CheckpointWait(ctx, delay) {
						return nil, nil, ctx.Err()
					}
					delay = checkpointBackoff(delay)
				}
			}
		}
		checkpointIssue(issue, "load_wait")
		if !CheckpointWait(ctx, delay) {
			break
		}
		delay = checkpointBackoff(delay)
	}
	return nil, nil, ctx.Err()
}

// Save resolves any frozen uncertain attempt before accepting a newer position.
// If it commits an older pending value, the caller's newer value remains dirty
// and must be offered again on the next save tick. Current returns what actually
// committed. This bounds uncertain state to one frozen write plus caller latest.
func (s *CheckpointSession) Save(ctx context.Context, position []byte) error {
	if s.pending == nil {
		if bytes.Equal(position, s.current.Position) {
			return nil
		}
		next := s.current.Clone()
		next.Position = append([]byte(nil), position...)
		var err error
		next.Revision, err = checkpointToken()
		if err != nil {
			return err
		}
		next.Attempt, err = checkpointToken()
		if err != nil {
			return err
		}
		s.pending = &next
	}
	return s.commit(ctx)
}

func (s *CheckpointSession) Current() Checkpoint { return s.current.Clone() }

func (s *CheckpointSession) commit(ctx context.Context) error {
	next := s.pending.Clone()
	ok, err := s.store.CompareAndSwap(ctx, s.current.CheckpointFence, next)
	if err != nil {
		actual, found, readErr := s.store.Load(ctx)
		if readErr != nil {
			return err
		}
		if found && actual.CheckpointFence == next.CheckpointFence && actual.Attempt == next.Attempt && actual.SchemaVersion == next.SchemaVersion && actual.SourceFingerprint == next.SourceFingerprint && bytes.Equal(actual.Position, next.Position) {
			ok, err = true, nil
		} else if (!found && s.current.CheckpointFence != (CheckpointFence{})) || (found && actual.CheckpointFence != s.current.CheckpointFence) {
			return ErrCheckpointConflict
		} else {
			return err
		}
	}
	if !ok {
		return ErrCheckpointConflict
	}
	s.current, s.pending = next, nil
	return nil
}

func checkpointToken() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func checkpointIssue(issue func(string), code string) {
	if issue != nil {
		issue(code)
	}
}

// CheckpointWait is a cancellation-aware jittered wait. Jitter never shortens a
// minimum vendor delay or the 30-second minimum save interval.
func CheckpointWait(ctx context.Context, minimum time.Duration) bool {
	var b [1]byte
	_, _ = rand.Read(b[:])
	extra := time.Duration(b[0]) * (minimum / 2560)
	d := minimum + extra
	if d < minimum {
		d = time.Duration(1<<63 - 1)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func checkpointBackoff(d time.Duration) time.Duration {
	if d < 30*time.Second {
		d *= 2
	}
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

package utils

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type sessionStore struct {
	mu                  sync.Mutex
	value               Checkpoint
	found               bool
	loadError           error
	saveError           error
	refuse              bool
	lostReply           bool
	unavailableReadback bool
	tamperAttempt       bool
	attempts            []Checkpoint
}

func (s *sessionStore) Load(ctx context.Context) (Checkpoint, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return Checkpoint{}, false, err
	}
	if s.loadError != nil {
		return Checkpoint{}, false, s.loadError
	}
	return s.value.Clone(), s.found, nil
}
func (s *sessionStore) CompareAndSwap(ctx context.Context, expected CheckpointFence, next Checkpoint) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	s.attempts = append(s.attempts, next.Clone())
	if s.refuse && !s.found {
		return false, ErrCheckpointAllocationRefused
	}
	if s.saveError != nil {
		return false, s.saveError
	}
	if s.found && s.value.CheckpointFence == next.CheckpointFence && s.value.Attempt == next.Attempt && bytes.Equal(s.value.Position, next.Position) {
		return true, nil
	}
	if (!s.found && expected != (CheckpointFence{})) || (s.found && s.value.CheckpointFence != expected) {
		return false, nil
	}
	s.value = next.Clone()
	s.found = true
	if s.lostReply {
		if s.tamperAttempt {
			s.value.Attempt = "wrong-attempt"
		}
		s.lostReply = false
		if s.unavailableReadback {
			s.loadError = errors.New("unavailable")
		}
		return false, errors.New("lost reply")
	}
	return true, nil
}
func openSession(t *testing.T, s CheckpointStore) (*CheckpointSession, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, raw, err := OpenCheckpoint(ctx, s, 1, [32]byte{1}, func() ([]byte, error) { return []byte("initial"), nil }, func([]byte) error { return nil }, nil)
	require.NoError(t, err)
	return session, raw
}

func TestCheckpointSessionRetainsUncertainAttemptAndNewerPosition(t *testing.T) {
	store := &sessionStore{}
	session, _ := openSession(t, store)
	store.lostReply = true
	store.unavailableReadback = true
	require.Error(t, session.Save(context.Background(), []byte("first")))
	frozen := session.pending.Clone()
	require.Equal(t, "initial", string(session.Current().Position))
	store.mu.Lock()
	store.loadError = nil
	store.mu.Unlock()
	require.NoError(t, session.Save(context.Background(), []byte("newer")))
	require.Equal(t, frozen.CheckpointFence, session.Current().CheckpointFence)
	require.Equal(t, "first", string(session.Current().Position), "uncertain first attempt resolves before newer state")
	require.NoError(t, session.Save(context.Background(), []byte("newer")))
	require.Equal(t, "newer", string(session.Current().Position))
	require.NotEqual(t, frozen.Attempt, session.Current().Attempt)
	require.NotEqual(t, frozen.Revision, session.Current().Revision)
}

func TestCheckpointSessionLostReplyExactFenceAndAttempt(t *testing.T) {
	store := &sessionStore{}
	session, _ := openSession(t, store)
	store.lostReply = true
	require.NoError(t, session.Save(context.Background(), []byte("confirmed")))
	require.Equal(t, "confirmed", string(session.Current().Position))
	store.mu.Lock()
	store.value.Attempt = "different-attempt"
	store.mu.Unlock()
	// A successor can reuse neither this nonce/revision nor the attempt.
	store.saveError = errors.New("unknown outcome")
	require.Error(t, session.Save(context.Background(), []byte("newer")))
	require.Equal(t, "confirmed", string(session.Current().Position))
}

func TestCheckpointSessionConflictNeverRebasesCandidate(t *testing.T) {
	store := &sessionStore{}
	first, _ := openSession(t, store)
	second, _ := openSession(t, store)
	require.NotEqual(t, first.Current().Nonce, second.Current().Nonce)
	require.ErrorIs(t, first.Save(context.Background(), []byte("stale")), ErrCheckpointConflict)
	require.Equal(t, "initial", string(store.value.Position))
	third, raw := openSession(t, store)
	require.Equal(t, "initial", string(raw))
	require.NoError(t, third.Save(context.Background(), []byte("fresh")))
	require.Equal(t, "fresh", string(store.value.Position))
}

func TestCheckpointSessionUnavailableIsNotMissing(t *testing.T) {
	store := &sessionStore{loadError: errors.New("unavailable")}
	var initials atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, _, err := OpenCheckpoint(ctx, store, 1, [32]byte{1}, func() ([]byte, error) { initials.Add(1); return []byte("now"), nil }, func([]byte) error { return nil }, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, initials.Load())
	require.Empty(t, store.attempts)
}

func TestCheckpointSessionMissingBoundaryChosenOnceAcrossClaimRetry(t *testing.T) {
	store := &sessionStore{saveError: errors.New("retry")}
	var calls atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		require.Eventually(t, func() bool { store.mu.Lock(); defer store.mu.Unlock(); return len(store.attempts) > 0 }, time.Second, time.Millisecond)
		store.mu.Lock()
		store.saveError = nil
		store.mu.Unlock()
	}()
	_, raw, err := OpenCheckpoint(ctx, store, 1, [32]byte{1}, func() ([]byte, error) { calls.Add(1); return []byte("fixed-boundary"), nil }, func([]byte) error { return nil }, nil)
	<-done
	require.NoError(t, err)
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, "fixed-boundary", string(raw))
	require.GreaterOrEqual(t, len(store.attempts), 2)
	require.Equal(t, store.attempts[0].Attempt, store.attempts[1].Attempt)
	require.Equal(t, store.attempts[0].Revision, store.attempts[1].Revision)
}

func TestCheckpointSessionRefusalAndSourceGeneration(t *testing.T) {
	refused := &sessionStore{refuse: true}
	session, raw := openSession(t, refused)
	require.Nil(t, session)
	require.Equal(t, "initial", string(raw))
	require.False(t, refused.found)
	store := &sessionStore{found: true, value: Checkpoint{SchemaVersion: 1, SourceFingerprint: [32]byte{9}, Position: []byte("other-source"), CheckpointFence: CheckpointFence{Nonce: "old", Revision: "old"}}}
	session, raw = openSession(t, store)
	require.Equal(t, "initial", string(raw))
	require.Equal(t, [32]byte{1}, session.Current().SourceFingerprint)
	// Same source credential rotation is deliberately absent from identity.
	_, raw = openSession(t, store)
	require.Equal(t, "initial", string(raw))
}

func TestCheckpointSessionCorruptStateWaitsInsteadOfSkipping(t *testing.T) {
	store := &sessionStore{found: true, value: Checkpoint{SchemaVersion: 99, SourceFingerprint: [32]byte{1}, Position: []byte("corrupt"), CheckpointFence: CheckpointFence{Nonce: "n", Revision: "r"}}}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _, err := OpenCheckpoint(ctx, store, 1, [32]byte{1}, func() ([]byte, error) { t.Error("must not select now for corrupt state"); return nil, nil }, func([]byte) error { return nil }, nil)
	require.Error(t, err)
	require.Empty(t, store.attempts)
}

func TestCheckpointSessionAmbiguousReplyRequiresExactAttempt(t *testing.T) {
	store := &sessionStore{}
	session, _ := openSession(t, store)
	store.lostReply = true
	store.tamperAttempt = true
	require.ErrorIs(t, session.Save(context.Background(), []byte("not-proved")), ErrCheckpointConflict)
	require.Equal(t, "initial", string(session.Current().Position))
}

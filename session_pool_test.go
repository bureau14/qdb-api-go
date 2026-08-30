package qdb

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"
)

// testDialer dials the cheapest handle the C API makes, one worker thread,
// and counts its calls. failNext fails that many dials up front, which is
// the only way to make a dial fail without a server fault.
type testDialer struct {
	f        *SessionFactory
	calls    atomic.Int64
	failNext atomic.Int64
}

func newTestDialer() *testDialer {
	opts := NewHandleOptions().
		WithClusterUri(insecureURI).
		WithCompression(CompNone).
		WithClientMaxParallelism(1).
		WithTimeout(5 * time.Second)

	return &testDialer{f: NewSessionFactory(opts)}
}

func (d *testDialer) dial(context.Context) (Session, error) {
	d.calls.Add(1)
	if d.failNext.Load() > 0 {
		d.failNext.Add(-1)

		return HandleType{}, ErrConnectionRefused
	}

	return d.f.NewSession()
}

// newTestPool builds a pool over d and drains it on cleanup, so a failing
// test never leaves sessions open against the shared cluster.
func newTestPool(t testHelper, d *testDialer, o *SessionPoolOptions) *SessionPool {
	t.Helper()
	p, err := NewSessionPool(d.f, o.WithDialer(d.dial))
	require.NoError(t, err)

	drain := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		require.NoError(t, p.Close(ctx))
	}
	switch v := t.(type) {
	case *testing.T:
		v.Cleanup(drain)
	case *rapid.T:
		v.Cleanup(drain)
	}

	return p
}

// waitClosed waits for the closes in flight, which run on their own
// goroutines, so that stats can be compared exactly.
func waitClosed(t testHelper, p *SessionPool) {
	t.Helper()
	require.Eventually(t, func() bool { return p.Stats().Closing == 0 }, 10*time.Second, time.Millisecond)
}

// Pool options never reach the C API, so unlike handle options they are
// judged at construction; a mistake is fatal, never retried.
func TestSessionPoolOptionsRejected(t *testing.T) {
	d := newTestDialer()
	tests := []struct {
		name string
		opts *SessionPoolOptions
	}{
		{"max sessions zero", NewSessionPoolOptions().WithMaxSessions(0)},
		{"negative idle timeout", NewSessionPoolOptions().WithIdleTimeout(-time.Second)},
		{"negative max lifetime", NewSessionPoolOptions().WithMaxLifetime(-time.Second)},
		{"negative reap interval", NewSessionPoolOptions().WithReapInterval(-time.Second)},
		{"nil clock", NewSessionPoolOptions().WithClock(nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewSessionPool(d.f, tt.opts)
			require.ErrorIs(t, err, ErrInvalidArgument)
			require.True(t, IsFatal(err))
		})
	}

	_, err := NewSessionPool(nil, nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

// Twelve goroutines over three slots: the cap holds under contention and
// every lease comes back.
func TestSessionPoolConcurrentAcquireHonorsMax(t *testing.T) {
	const maxSessions, workers, rounds = 3, 12, 5
	p := newTestPool(t, newTestDialer(), NewSessionPoolOptions().WithMaxSessions(maxSessions))

	var overCap atomic.Bool
	errs := make(chan error, workers*rounds)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range rounds {
				l, err := p.Acquire(context.Background())
				errs <- err
				if err != nil {
					continue
				}
				s := p.Stats()
				if s.InUse+s.Idle+s.Dialing > maxSessions {
					overCap.Store(true)
				}
				l.Release()
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.False(t, overCap.Load())

	waitClosed(t, p)
	s := p.Stats()
	require.Zero(t, s.InUse)
	require.LessOrEqual(t, s.Idle, maxSessions)
}

// A dial that fails gives its slot back at once: the next Acquire on a
// one-slot pool must not wait for it.
func TestSessionPoolFailedDialConsumesNoSlot(t *testing.T) {
	d := newTestDialer()
	p := newTestPool(t, d, NewSessionPoolOptions().WithMaxSessions(1))
	d.failNext.Store(1)

	_, err := p.Acquire(context.Background())
	require.ErrorIs(t, err, ErrConnectionRefused)
	require.Equal(t, SessionPoolStats{}, p.Stats())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	l, err := p.Acquire(ctx)
	require.NoError(t, err)
	l.Release()
	require.Equal(t, SessionPoolStats{Idle: 1, Dialed: 1}, p.Stats())
}

// Close never takes a session from its holder: it reports what is still
// out when ctx ends, refuses new acquires, and completes once the lease
// comes back.
func TestSessionPoolCloseWaitsForLeases(t *testing.T) {
	p := newTestPool(t, newTestDialer(), NewSessionPoolOptions().WithMaxSessions(1))
	l, err := p.Acquire(context.Background())
	require.NoError(t, err)

	short, cancelShort := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelShort()
	require.ErrorIs(t, p.Close(short), context.DeadlineExceeded)

	_, err = p.Acquire(context.Background())
	require.ErrorIs(t, err, ErrUninitialized)
	require.True(t, IsFatal(err))
	require.Equal(t, SessionPoolStats{InUse: 1, Dialed: 1}, p.Stats())

	l.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, p.Close(ctx))
	require.Equal(t, SessionPoolStats{Dialed: 1}, p.Stats())
}

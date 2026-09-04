package qdb

import (
	"context"
	"slices"
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
	calls    atomic.Uint64
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
// judged at construction; a mistake is not retryable.
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
	require.Equal(t, SessionPoolStats{InUse: 1, Dialed: 1}, p.Stats())

	l.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, p.Close(ctx))
	require.Equal(t, SessionPoolStats{Dialed: 1}, p.Stats())
}

// The reaper goroutine runs on the wall clock and closes an idle session
// on its own; the cleanup's Close pins that it stops.
func TestSessionPoolReaperClosesIdle(t *testing.T) {
	p := newTestPool(t, newTestDialer(), NewSessionPoolOptions().
		WithIdleTimeout(50*time.Millisecond).
		WithReapInterval(10*time.Millisecond))
	l, err := p.Acquire(context.Background())
	require.NoError(t, err)
	l.Release()

	require.Eventually(t, func() bool { return p.Stats().Idle == 0 }, 5*time.Second, time.Millisecond)
	waitClosed(t, p)
	require.Equal(t, SessionPoolStats{Dialed: 1}, p.Stats())
}

// The closer runs once per closed session, after the lease ended, and for
// none that is merely idle: the caller told when a session begins is told
// when it ends.
func TestSessionPoolCloserRunsOncePerClosedSession(t *testing.T) {
	var closes atomic.Uint64
	d := newTestDialer()
	var p *SessionPool
	closer := func(s Session) error {
		require.Zero(t, p.Stats().InUse, "the closer runs after the lease ended")
		closes.Add(1)

		return s.Close()
	}
	p = newTestPool(t, d, NewSessionPoolOptions().WithMaxSessions(1).WithCloser(closer))

	l, err := p.Acquire(context.Background())
	require.NoError(t, err)
	l.Discard()
	waitClosed(t, p)
	require.Equal(t, uint64(1), closes.Load())

	l, err = p.Acquire(context.Background())
	require.NoError(t, err)
	l.Release()
	require.Equal(t, uint64(1), closes.Load(), "an idle session is not closed")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, p.Close(ctx))
	require.Equal(t, d.calls.Load(), closes.Load(), "every dialed session is closed once")
}

// An error that is not IsBadSession says nothing against the session: the
// cluster judged the request, or the op failed on its own. Do hands the
// session back and the next Do reuses it without a dial.
func TestSessionPoolDoReusesSessionAfterError(t *testing.T) {
	invalidQuery := func(s Session) error {
		_, err := s.Query("select").Execute()

		return err
	}
	tests := []struct {
		name string
		op   func(Session) error
		want error
	}{
		{"query judged by the cluster", invalidQuery, ErrInvalidQuery},
		{"judged code from the op", func(Session) error { return ErrColumnNotFound }, ErrColumnNotFound},
		{"context deadline from the op", func(Session) error { return context.DeadlineExceeded }, context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDialer()
			p := newTestPool(t, d, NewSessionPoolOptions().WithMaxSessions(1))
			ctx := context.Background()

			err := p.Do(ctx, tt.op)
			require.ErrorIs(t, err, tt.want)
			require.False(t, IsBadSession(err))
			require.Equal(t, SessionPoolStats{Idle: 1, Dialed: 1}, p.Stats())

			require.NoError(t, p.Do(ctx, func(Session) error { return nil }))
			require.Equal(t, uint64(1), d.calls.Load(), "the idle session is reused, not dialed")
		})
	}
}

// fakeClock is the clock behind WithClock; advance moves it, nothing
// sleeps.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// poolModel is the oracle of TestSessionPoolInvariants. pool is a real
// SessionPool dialing real sessions; the model keeps its own book of what
// that pool should hold. Every action drives the pool through its public
// methods, then updates the book the way the pool is specified to
// behave; check compares pool.Stats() with the book. The clock is faked
// so that ages move without waiting, and the reaper is off so that
// sessions expire only inside Reap and Acquire, at moments the book sees.
//
// Sessions compare by value, that is by C pointer, which the C API reuses
// after a close; an idle session is open, though, so a match against the
// book's idle set is unambiguous.
type poolModel struct {
	pool      *SessionPool // the pool under test
	dialer    *testDialer
	clock     *fakeClock
	opts      *SessionPoolOptions
	leases    []*Lease      // what the pool should have leased
	idle      []idleSession // what it should hold idle, in its order
	discarded uint64        // what Stats().Discarded should read
}

// newPoolModel draws the sizing, builds the real pool over the fake
// clock, and starts with an empty book.
func newPoolModel(t *rapid.T) *poolModel {
	clock := newFakeClock()
	dialer := newTestDialer()
	opts := NewSessionPoolOptions().
		WithMaxSessions(rapid.IntRange(1, 4).Draw(t, "max_sessions")).
		WithIdleTimeout(time.Duration(rapid.IntRange(1, 100).Draw(t, "idle_timeout")) * time.Second).
		WithMaxLifetime(time.Duration(rapid.IntRange(1, 300).Draw(t, "max_lifetime")) * time.Second).
		WithReapInterval(0).
		WithClock(clock.Now)

	return &poolModel{pool: newTestPool(t, dialer, opts), dialer: dialer, clock: clock, opts: opts}
}

func (m *poolModel) expired(e idleSession) bool {
	return m.clock.Now().Sub(e.lastUsed) >= m.opts.idleTimeout
}

// acquire drives Acquire and books what the pool must have done: reused
// the freshest idle session when it has not expired; otherwise every
// idle session has expired and was closed, and a slot was either free to
// dial into or not.
func (m *poolModel) acquire(t *rapid.T) {
	n := len(m.idle)
	if n > 0 && !m.expired(m.idle[n-1]) {
		m.acquireIdle(t)

		return
	}
	m.idle = nil
	if len(m.leases) >= m.opts.maxSessions {
		m.acquireFull(t)

		return
	}
	m.acquireFresh(t)
}

func (m *poolModel) acquireIdle(t *rapid.T) {
	before := m.dialer.calls.Load()
	l, err := m.pool.Acquire(context.Background())
	require.NoError(t, err)
	require.Equal(t, before, m.dialer.calls.Load(), "an idle session is reused, not dialed")
	n := len(m.idle)
	require.Equal(t, m.idle[n-1].session, l.Session(), "the freshest idle session is reused")
	m.idle = m.idle[:n-1]
	m.leases = append(m.leases, l)
}

func (m *poolModel) acquireFresh(t *rapid.T) {
	before := m.dialer.calls.Load()
	l, err := m.pool.Acquire(context.Background())
	require.NoError(t, err)
	require.Equal(t, before+1, m.dialer.calls.Load(), "a free slot with nothing idle dials")
	m.leases = append(m.leases, l)
}

func (m *poolModel) acquireFull(t *rapid.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := m.pool.Acquire(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

// pick removes and returns one leased session; Skip when none is out.
func (m *poolModel) pick(t *rapid.T) *Lease {
	if len(m.leases) == 0 {
		t.Skip("nothing leased")
	}
	i := rapid.IntRange(0, len(m.leases)-1).Draw(t, "lease")
	l := m.leases[i]
	m.leases = slices.Delete(m.leases, i, i+1)

	return l
}

// released mirrors Release: back to the idle set unless outlived.
func (m *poolModel) released(l *Lease) {
	now := m.clock.Now()
	if now.Sub(l.created) >= m.opts.maxLifetime {
		return
	}
	m.idle = append(m.idle, idleSession{session: l.Session(), created: l.created, lastUsed: now})
}

func (m *poolModel) release(t *rapid.T) {
	l := m.pick(t)
	l.Release()
	m.released(l)
}

func (m *poolModel) discard(t *rapid.T) {
	m.pick(t).Discard()
	m.discarded++
}

func (m *poolModel) done(t *rapid.T) {
	// one of each: success, judged input, judged operation, retryable but
	// not poisoning, the wire failed, not a C API error
	fates := []error{nil, ErrInvalidQuery, ErrColumnNotFound, ErrTryAgain, ErrConnectionRefused, context.DeadlineExceeded}
	fate := rapid.SampledFrom(fates).Draw(t, "fate")
	l := m.pick(t)
	l.Done(fate)
	if IsBadSession(fate) {
		m.discarded++

		return
	}
	m.released(l)
}

func (m *poolModel) advance(t *rapid.T) {
	m.clock.advance(time.Duration(rapid.IntRange(0, 120).Draw(t, "seconds")) * time.Second)
}

func (m *poolModel) reap(*rapid.T) {
	m.pool.Reap()
	m.idle = slices.DeleteFunc(m.idle, m.expired)
}

func (m *poolModel) check(t *rapid.T) {
	waitClosed(t, m.pool)
	s := m.pool.Stats()
	require.Equal(t, len(m.leases), s.InUse)
	require.Equal(t, len(m.idle), s.Idle)
	require.Zero(t, s.Dialing)
	require.Equal(t, m.discarded, s.Discarded)
	require.Equal(t, m.dialer.calls.Load(), s.Dialed)
	require.LessOrEqual(t, s.InUse+s.Idle, m.opts.maxSessions)
}

// The pool against a model of itself under a fake clock: the cap, the
// reuse order, idle expiry, lifetime and the counters hold under any
// interleaving of the operations.
func TestSessionPoolInvariants(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		m := newPoolModel(rt)
		rt.Repeat(map[string]func(*rapid.T){
			"":        m.check,
			"acquire": m.acquire,
			"release": m.release,
			"discard": m.discard,
			"done":    m.done,
			"advance": m.advance,
			"reap":    m.reap,
		})
		for _, l := range m.leases {
			l.Release()
		}
	})
}

// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

/*
	#include <qdb/client.h>
*/
import "C"

import (
	"context"
	"sync"
	"time"
)

// SessionPoolOptions sizes a SessionPool. Options are immutable: every
// With... method returns a copy.
//
// Each handle has a thread pool of its own, there to make a single
// operation faster, over several connections. Pooling handles is what
// lets one user run operations concurrently, each on its own handle.
type SessionPoolOptions struct {
	maxSessions  int
	idleTimeout  time.Duration
	maxLifetime  time.Duration
	reapInterval time.Duration
	now          func() time.Time
	dialer       func(context.Context) (Session, error)
	closer       func(Session) error
}

// NewSessionPoolOptions returns options with the defaults: 8 sessions, 5
// minutes idle, 15 minutes lifetime, a reaper every 10 seconds, the wall
// clock, the factory as dialer, and Session.Close as closer.
//
// Example:
//
//	pool, err := qdb.NewSessionPool(factory, qdb.NewSessionPoolOptions().
//	    WithMaxSessions(16).
//	    WithIdleTimeout(time.Minute))
func NewSessionPoolOptions() *SessionPoolOptions {
	return &SessionPoolOptions{
		maxSessions:  8,
		idleTimeout:  5 * time.Minute,
		maxLifetime:  15 * time.Minute,
		reapInterval: 10 * time.Second,
		now:          time.Now,
	}
}

// WithMaxSessions caps the sessions leased or idle at once. Must be at
// least 1.
func (o *SessionPoolOptions) WithMaxSessions(n int) *SessionPoolOptions {
	opts := *o
	opts.maxSessions = n

	return &opts
}

// WithIdleTimeout closes a session that has sat idle this long. Zero means
// never.
func (o *SessionPoolOptions) WithIdleTimeout(d time.Duration) *SessionPoolOptions {
	opts := *o
	opts.idleTimeout = d

	return &opts
}

// WithMaxLifetime closes a session older than this when it is released,
// so a handle that has degraded is not kept forever. Zero means never.
func (o *SessionPoolOptions) WithMaxLifetime(d time.Duration) *SessionPoolOptions {
	opts := *o
	opts.maxLifetime = d

	return &opts
}

// WithReapInterval sets how often the pool's own goroutine closes idle
// sessions past the idle timeout. Zero runs no goroutine; the caller then
// reaps through Reap, or relies on Acquire, which never hands out an
// expired session. The interval ticks on the wall clock even when
// WithClock is set, so tests that fake the clock also disable the reaper.
func (o *SessionPoolOptions) WithReapInterval(d time.Duration) *SessionPoolOptions {
	opts := *o
	opts.reapInterval = d

	return &opts
}

// WithClock replaces the clock every age is measured against. Intended
// for testing only.
func (o *SessionPoolOptions) WithClock(now func() time.Time) *SessionPoolOptions {
	opts := *o
	opts.now = now

	return &opts
}

// WithDialer replaces the session factory as the source of sessions.
// Intended for tests, and for callers that wrap the dial, for instance
// under a deadline. The default ignores the context: the C API cannot
// cancel a connect in flight.
func (o *SessionPoolOptions) WithDialer(dial func(context.Context) (Session, error)) *SessionPoolOptions {
	opts := *o
	opts.dialer = dial

	return &opts
}

// WithCloser replaces Session.Close as the way a session is closed: on
// discard, idle or lifetime expiry, and at Close. Intended for callers
// that wrap the close, for instance to account for the session once
// qdb_close has returned, which can be minutes after the lease ended. The
// pool still runs it on its own goroutine.
func (o *SessionPoolOptions) WithCloser(closer func(Session) error) *SessionPoolOptions {
	opts := *o
	opts.closer = closer

	return &opts
}

// validateSessionPoolOptions rejects what the pool cannot honour. Unlike
// handle options, which the C API judges at dial time, these never reach
// the C API, so the check is made here, once.
func validateSessionPoolOptions(o *SessionPoolOptions) error {
	if o.maxSessions < 1 {
		return wrapError(C.qdb_e_invalid_argument, "new_session_pool", "option", "max_sessions", "value", o.maxSessions)
	}
	if o.now == nil {
		return wrapError(C.qdb_e_invalid_argument, "new_session_pool", "option", "clock", "reason", "nil")
	}
	durations := []struct {
		name  string
		value time.Duration
	}{
		{"idle_timeout", o.idleTimeout},
		{"max_lifetime", o.maxLifetime},
		{"reap_interval", o.reapInterval},
	}
	for _, d := range durations {
		if d.value < 0 {
			return wrapError(C.qdb_e_invalid_argument, "new_session_pool", "option", d.name, "value", d.value)
		}
	}

	return nil
}

// factoryDialer is the default dialer: one NewSession per call, the
// context unused because the C API cannot cancel a connect.
func factoryDialer(f *SessionFactory) func(context.Context) (Session, error) {
	return func(context.Context) (Session, error) {
		return f.NewSession()
	}
}

// sessionPoolClosedError is what Acquire returns after Close. The code is
// one IsRetryable rejects, so a caller's retry loop stops.
func sessionPoolClosedError() error {
	return wrapError(C.qdb_e_uninitialized, "session_pool_acquire", "reason", "closed")
}

// SessionPoolStats is a snapshot of a SessionPool. InUse plus Idle plus
// Dialing never exceeds MaxSessions. Closing holds no slot, but each close
// still holds its cluster sessions until qdb_close returns, which can take
// minutes.
type SessionPoolStats struct {
	InUse     int
	Idle      int
	Dialing   int
	Closing   int
	Dialed    uint64 // sessions dialed successfully, ever
	Discarded uint64 // leases discarded, ever
}

// idleSession is a session waiting to be reused; lastUsed is when it was
// released.
type idleSession struct {
	session  Session
	created  time.Time
	lastUsed time.Time
}

// acquireStep is the outcome of one attempt to acquire under the lock:
// exactly one of the three is set, and the caller acts on it once the
// lock is released.
type acquireStep struct {
	lease *Lease        // an idle session was taken
	dial  bool          // a slot was reserved; the caller dials into it
	wait  chan struct{} // the pool is full; the caller waits on it
}

// SessionPool is a bounded set of sessions dialed from one factory: at
// most MaxSessions leased or idle at once, dialed on demand, never at
// construction.
//
// A handle must not be treated as thread-safe: never share a leased
// session between goroutines, or synchronize access to it yourself. The
// Lease is the token of that ownership: the pool never closes a leased
// session, not even in Close. Every close runs on its own goroutine
// because qdb_close joins the handle's worker threads and can block for
// minutes. Acquire's context bounds the wait for a free slot; the dial
// itself cannot be cancelled (see WithDialer).
//
// Example:
//
//	pool, err := qdb.NewSessionPool(factory, qdb.NewSessionPoolOptions())
//	if err != nil {
//	    return err
//	}
//	defer pool.Close(ctx)
//
//	err = pool.Do(ctx, func(s qdb.Session) error {
//	    return s.Blob("alias").Put(data, qdb.NeverExpires())
//	})
//
//	// Or, holding the session across several calls:
//	lease, err := pool.Acquire(ctx)
//	if err != nil {
//	    return err
//	}
//	err = lease.Session().Blob("alias").Put(data, qdb.NeverExpires())
//	lease.Done(err)
type SessionPool struct {
	opts  *SessionPoolOptions
	dial  func(context.Context) (Session, error)
	close func(Session) error

	// reaperStop ends the reaper goroutine and reaperDone closes once it
	// has ended; both are nil when no reaper runs. reaperOnce lets Close be
	// called more than once.
	reaperStop chan struct{}
	reaperDone chan struct{}
	reaperOnce sync.Once

	mu sync.Mutex
	// idle is ordered by lastUsed, oldest first: expiry removes a prefix
	// and reuse takes the tail, the session most likely still warm.
	idle      []idleSession
	leased    int    // sessions checked out, each holding a slot
	dialing   int    // dials in flight, each holding a slot
	closing   int    // closes in flight, holding no slot
	dialed    uint64 // dials that succeeded, ever
	discarded uint64 // leases discarded, ever
	closed    bool
	// changed is closed and replaced on every state change. A waiter takes
	// the current channel under mu and selects on it and its context, which
	// a sync.Cond could not do.
	changed chan struct{}
}

// NewSessionPool returns an empty pool dialing from f; nothing is dialed
// before Acquire. Nil options are the defaults. An option the pool cannot
// honour is rejected with an error IsRetryable rejects; a factory
// misconfiguration surfaces on the first Acquire instead, judged by the C
// API at dial time.
func NewSessionPool(f *SessionFactory, o *SessionPoolOptions) (*SessionPool, error) {
	if o == nil {
		o = NewSessionPoolOptions()
	}
	err := validateSessionPoolOptions(o)
	if err != nil {
		return nil, err
	}
	dial := o.dialer
	if dial == nil && f == nil {
		return nil, wrapError(C.qdb_e_invalid_argument, "new_session_pool", "option", "factory", "reason", "nil")
	}
	if dial == nil {
		dial = factoryDialer(f)
	}
	closer := o.closer
	if closer == nil {
		closer = Session.Close
	}
	p := &SessionPool{opts: o, dial: dial, close: closer, changed: make(chan struct{})}
	if o.reapInterval > 0 && o.idleTimeout > 0 {
		p.reaperStop = make(chan struct{})
		p.reaperDone = make(chan struct{})
		go p.runReaper(o.reapInterval)
	}

	return p, nil
}

// Acquire returns a leased session: the freshest idle one, else a freshly
// dialed one when a slot is free, else it waits for a release or for ctx
// to end, returning ctx.Err(). After Close it returns an error IsRetryable
// rejects, so a retry loop stops.
func (p *SessionPool) Acquire(ctx context.Context) (*Lease, error) {
	for {
		step, err := p.tryAcquire()
		if err != nil {
			return nil, err
		}
		if step.lease != nil {
			return step.lease, nil
		}
		if step.dial {
			return p.dialLease(ctx)
		}
		select {
		case <-step.wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Do runs op on a leased session and returns op's error, the lease ended
// through Done so that the error decides the session's fate. The lease
// ends in a defer: an op that panics still gives its slot back, or Close
// would wait for it forever.
func (p *SessionPool) Do(ctx context.Context, op func(Session) error) (err error) {
	l, err := p.Acquire(ctx)
	if err != nil {
		return err
	}
	defer func() { l.Done(err) }()

	return op(l.Session())
}

// Reap closes every idle session past the idle timeout. The reaper
// goroutine calls it on its interval; a pool built without one leaves it
// to the caller.
func (p *SessionPool) Reap() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reapLocked()
}

// Close stops the reaper, refuses further acquires, closes every idle
// session and waits for the leases, dials and closes still outstanding,
// until ctx ends. It returns ctx.Err() when something was still
// outstanding: those sessions are closed by their holders' Release or
// Discard, or by their own close goroutine, whenever they finish. Calling
// Close again waits again.
func (p *SessionPool) Close(ctx context.Context) error {
	p.stopReaper()
	p.shutdown()

	return p.awaitDrained(ctx)
}

// Stats returns a snapshot.
func (p *SessionPool) Stats() SessionPoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.statsLocked()
}

// takeIdleLocked hands out the freshest idle session. An expired one is
// closed instead and, the list being ordered by lastUsed, an expired
// freshest one means every idle session has expired.
func (p *SessionPool) takeIdleLocked() (*Lease, bool) {
	now := p.opts.now()
	for n := len(p.idle); n > 0; n = len(p.idle) {
		e := p.idle[n-1]
		p.idle = p.idle[:n-1]
		if p.expired(e.lastUsed, now) {
			p.closeSessionLocked(e.session)

			continue
		}
		p.leased++

		return &Lease{pool: p, session: e.session, created: e.created}, true
	}

	return nil, false
}

// slotFreeLocked reports whether another session may be dialed. Leased,
// idle and dialing sessions all hold a slot: a dial reserves its slot
// before it connects, so concurrent acquires cannot overshoot the cap
// while a connect is in flight.
func (p *SessionPool) slotFreeLocked() bool {
	return p.leased+len(p.idle)+p.dialing < p.opts.maxSessions
}

// tryAcquireLocked makes one attempt: an idle session, else a reserved
// slot to dial into, else the channel to wait on. After Close it fails.
func (p *SessionPool) tryAcquireLocked() (acquireStep, error) {
	if p.closed {
		return acquireStep{}, sessionPoolClosedError()
	}
	l, ok := p.takeIdleLocked()
	if ok {
		return acquireStep{lease: l}, nil
	}
	if p.slotFreeLocked() {
		p.dialing++

		return acquireStep{dial: true}, nil
	}

	return acquireStep{wait: p.changed}, nil
}

func (p *SessionPool) tryAcquire() (acquireStep, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.tryAcquireLocked()
}

// admitDialedLocked settles a dial: on an error the slot goes back;
// otherwise the session is leased, or closed at once when the pool was
// closed while the dial was in flight.
func (p *SessionPool) admitDialedLocked(s Session, err error) (*Lease, error) {
	p.dialing--
	if err != nil {
		return nil, err
	}
	p.dialed++
	if p.closed {
		p.closeSessionLocked(s)

		return nil, sessionPoolClosedError()
	}
	p.leased++

	return &Lease{pool: p, session: s, created: p.opts.now()}, nil
}

// dialLease dials on the caller's goroutine, outside the lock because a
// dial blocks for as long as the connect takes, into the slot reserved by
// tryAcquireLocked.
func (p *SessionPool) dialLease(ctx context.Context) (*Lease, error) {
	s, err := p.dial(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.notifyLocked()

	return p.admitDialedLocked(s, err)
}

// closeSessionLocked closes s on its own goroutine: qdb_close joins the
// handle's threads and can block for minutes, and the pool must never wait
// on it. The handle is an opaque pointer owned by the C API, not Go
// memory, so handing it to another goroutine breaks no cgo rule; the C
// API's own rule, one close by the owner after every call on the handle
// has returned, holds because the lease ended before this runs.
func (p *SessionPool) closeSessionLocked(s Session) {
	p.closing++
	go func() {
		err := p.close(s)
		if err != nil {
			L().Debug("session pool: close failed", "error", err)
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.closing--
		p.notifyLocked()
	}()
}

func (p *SessionPool) shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.shutdownLocked()
}

// shutdownLocked refuses further acquires and closes every idle session.
func (p *SessionPool) shutdownLocked() {
	p.closed = true
	for _, e := range p.idle {
		p.closeSessionLocked(e.session)
	}
	p.idle = nil
	p.notifyLocked()
}

// outstanding reports whether a lease, dial or close is still out, and
// the channel that closes on the next state change.
func (p *SessionPool) outstanding() (busy bool, changed chan struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.leased > 0 || p.dialing > 0 || p.closing > 0, p.changed
}

// awaitDrained waits until nothing is outstanding, or until ctx ends.
func (p *SessionPool) awaitDrained(ctx context.Context) error {
	for {
		busy, wait := p.outstanding()
		if !busy {
			return nil
		}
		select {
		case <-wait:
		case <-ctx.Done():
			s := p.Stats()
			L().Warn("session pool closed with sessions outstanding", "in_use", s.InUse, "dialing", s.Dialing, "closing", s.Closing)

			return ctx.Err()
		}
	}
}

// reapLocked closes every idle session past the idle timeout.
func (p *SessionPool) reapLocked() {
	now := p.opts.now()
	kept := p.idle[:0]
	for _, e := range p.idle {
		if p.expired(e.lastUsed, now) {
			p.closeSessionLocked(e.session)
		} else {
			kept = append(kept, e)
		}
	}
	if len(kept) != len(p.idle) {
		p.notifyLocked()
	}
	p.idle = kept
}

func (p *SessionPool) statsLocked() SessionPoolStats {
	return SessionPoolStats{
		InUse:     p.leased,
		Idle:      len(p.idle),
		Dialing:   p.dialing,
		Closing:   p.closing,
		Dialed:    p.dialed,
		Discarded: p.discarded,
	}
}

// notifyLocked wakes every goroutine waiting for the state to change.
func (p *SessionPool) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// expired reports whether a session idle since lastUsed is past the idle
// timeout.
func (p *SessionPool) expired(lastUsed, now time.Time) bool {
	return p.opts.idleTimeout > 0 && now.Sub(lastUsed) >= p.opts.idleTimeout
}

// outlived reports whether a session created at created is past the
// maximum lifetime.
func (p *SessionPool) outlived(created, now time.Time) bool {
	return p.opts.maxLifetime > 0 && now.Sub(created) >= p.opts.maxLifetime
}

// runReaper reaps on every tick until stopped.
func (p *SessionPool) runReaper(interval time.Duration) {
	defer close(p.reaperDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			p.Reap()
		case <-p.reaperStop:
			return
		}
	}
}

// stopReaper ends the reaper goroutine and waits for it, once. It must
// not hold mu: Reap takes it.
func (p *SessionPool) stopReaper() {
	p.reaperOnce.Do(func() {
		if p.reaperStop == nil {
			return
		}
		close(p.reaperStop)
		<-p.reaperDone
	})
}

// Lease is one checked-out session. It belongs to the holder until
// Release or Discard; the pool never closes a leased session.
type Lease struct {
	pool    *SessionPool
	session Session
	created time.Time
	done    bool // guarded by pool.mu
}

// Session returns the leased session.
func (l *Lease) Session() Session {
	return l.session
}

// Release returns the session to the idle set, or closes it when it has
// outlived MaxLifetime or the pool is closed. A second Release or Discard
// is a no-op.
func (l *Lease) Release() {
	l.pool.mu.Lock()
	defer l.pool.mu.Unlock()
	l.releaseLocked()
}

// Discard closes the session; it is never handed out again. For a session
// that can no longer be trusted; Done decides that from the error.
func (l *Lease) Discard() {
	l.pool.mu.Lock()
	defer l.pool.mu.Unlock()
	l.discardLocked()
}

// Done ends the lease according to err, the outcome of the calls made on
// the session: Discard when IsBadSession holds, Release otherwise. An
// error that is not a C API error, a context deadline included, says
// nothing about the session and releases.
func (l *Lease) Done(err error) {
	if IsBadSession(err) {
		l.Discard()

		return
	}
	l.Release()
}

// releaseLocked puts the session back in the idle set, or closes it when
// it has outlived MaxLifetime or the pool is closed.
func (l *Lease) releaseLocked() {
	p := l.pool
	if !l.endLocked() {
		return
	}
	now := p.opts.now()
	if p.closed || p.outlived(l.created, now) {
		p.closeSessionLocked(l.session)
	} else {
		p.idle = append(p.idle, idleSession{session: l.session, created: l.created, lastUsed: now})
	}
	p.notifyLocked()
}

// discardLocked closes the session and counts the discard.
func (l *Lease) discardLocked() {
	p := l.pool
	if !l.endLocked() {
		return
	}
	p.discarded++
	p.closeSessionLocked(l.session)
	p.notifyLocked()
}

// endLocked marks the lease over and frees its slot; false when it already
// was, which is what makes Release and Discard idempotent.
func (l *Lease) endLocked() bool {
	if l.done {
		return false
	}
	l.done = true
	l.pool.leased--

	return true
}

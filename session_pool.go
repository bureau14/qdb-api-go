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
// Size with care. A session is one handle, and each handle owns a thread
// pool of client-max-parallelism threads (default: half the cores) plus a
// TCP connection pool per node address, so N sessions cost N times that.
// The cluster's session table is finite as well: once exhausted, qdbd
// refuses new sessions for fifteen minutes, and a session counts until its
// close returns, so a pool holds up to MaxSessions plus the closes still in
// flight. Tens of sessions is the right order of magnitude.
type SessionPoolOptions struct {
	maxSessions  int
	idleTimeout  time.Duration
	maxLifetime  time.Duration
	reapInterval time.Duration
	now          func() time.Time
	dialer       func(context.Context) (Session, error)
}

// NewSessionPoolOptions returns options with the defaults: 8 sessions, 5
// minutes idle, 15 minutes lifetime, a reaper every 10 seconds, the wall
// clock, and the factory as dialer.
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

// WithClock replaces the clock every age is measured against. Tests use it
// to advance time without waiting.
func (o *SessionPoolOptions) WithClock(now func() time.Time) *SessionPoolOptions {
	opts := *o
	opts.now = now

	return &opts
}

// WithDialer replaces the factory as the source of sessions: for wrapping
// the dial in a deadline, counting it, or injecting failures. The default
// calls SessionFactory.NewSession and ignores the context, because the C
// API cannot cancel a connect in flight; the context bounds only the wait
// for a free slot in Acquire.
func (o *SessionPoolOptions) WithDialer(dial func(context.Context) (Session, error)) *SessionPoolOptions {
	opts := *o
	opts.dialer = dial

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
// one for which IsFatal holds, so a caller's retry loop stops.
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

// SessionPool is a bounded set of sessions dialed from one factory: at
// most MaxSessions leased or idle at once, dialed on demand, never at
// construction.
//
// A handle is not thread-safe, so a session is used by one goroutine at a
// time and the Lease is the token of that ownership: the pool never closes
// a leased session, not even in Close. Every close runs on its own
// goroutine because qdb_close joins the handle's worker threads and can
// block for minutes. Acquire's context bounds the wait for a free slot;
// the dial itself cannot be cancelled (see WithDialer).
//
// Example:
//
//	pool, err := qdb.NewSessionPool(factory, qdb.NewSessionPoolOptions())
//	if err != nil {
//	    return err
//	}
//	defer pool.Close(ctx)
//
//	lease, err := pool.Acquire(ctx)
//	if err != nil {
//	    return err
//	}
//	err = lease.Session().Blob("alias").Put(data, qdb.NeverExpires())
//	if qdb.IsRetryable(err) {
//	    lease.Discard()
//	} else {
//	    lease.Release()
//	}
type SessionPool struct {
	opts *SessionPoolOptions
	dial func(context.Context) (Session, error)

	mu sync.Mutex
	// idle is ordered by lastUsed, oldest first: expiry removes a prefix
	// and reuse takes the tail, the session most likely still warm.
	idle      []idleSession
	leased    int
	dialing   int
	closing   int
	dialed    uint64
	discarded uint64
	closed    bool
	// changed is closed and replaced on every state change. A waiter takes
	// the current channel under mu and selects on it and its context, which
	// a sync.Cond could not do.
	changed chan struct{}
}

// NewSessionPool returns an empty pool dialing from f; nothing is dialed
// before Acquire. Nil options are the defaults. An option the pool cannot
// honour is rejected with an error for which IsFatal holds; a factory
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

	return &SessionPool{opts: o, dial: dial, changed: make(chan struct{})}, nil
}

// Acquire returns a leased session: the freshest idle one, else a freshly
// dialed one when a slot is free, else it waits for a release or for ctx
// to end, returning ctx.Err(). After Close it returns an error for which
// IsFatal holds.
func (p *SessionPool) Acquire(ctx context.Context) (*Lease, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()

			return nil, sessionPoolClosedError()
		}
		l, ok := p.takeIdleLocked()
		if ok {
			p.mu.Unlock()

			return l, nil
		}
		if p.leased+len(p.idle)+p.dialing < p.opts.maxSessions {
			p.dialing++
			p.mu.Unlock()

			return p.dialLease(ctx)
		}
		wait := p.changed
		p.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close refuses further acquires, closes every idle session and waits for
// the leases, dials and closes still outstanding, until ctx ends. It
// returns ctx.Err() when something was still outstanding: those sessions
// are closed by their holders' Release or Discard, or by their own close
// goroutine, whenever they finish. Calling Close again waits again.
func (p *SessionPool) Close(ctx context.Context) error {
	p.mu.Lock()
	p.shutdownLocked()
	p.mu.Unlock()

	return p.awaitDrained(ctx)
}

// Stats returns a snapshot.
func (p *SessionPool) Stats() SessionPoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	return SessionPoolStats{
		InUse:     p.leased,
		Idle:      len(p.idle),
		Dialing:   p.dialing,
		Closing:   p.closing,
		Dialed:    p.dialed,
		Discarded: p.discarded,
	}
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

// dialLease dials on the caller's goroutine, outside the lock because a
// dial blocks for as long as the connect takes, with the slot reserved
// through dialing. A dial that fails gives the slot back; one that
// completes after Close is closed at once.
func (p *SessionPool) dialLease(ctx context.Context) (*Lease, error) {
	s, err := p.dial(ctx)

	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.notifyLocked()
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

// closeSessionLocked closes s on its own goroutine: qdb_close joins the
// handle's threads and can block for minutes, and the pool must never wait
// on it. The handle is an opaque pointer owned by the C API, not Go
// memory, so handing it to another goroutine breaks no cgo rule; the C
// API's own rule, one close by the owner after every call on the handle
// has returned, holds because the lease ended before this runs.
func (p *SessionPool) closeSessionLocked(s Session) {
	p.closing++
	go func() {
		err := s.Close()
		if err != nil {
			L().Debug("session pool: close failed", "error", err)
		}
		p.mu.Lock()
		defer p.mu.Unlock()
		p.closing--
		p.notifyLocked()
	}()
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

// awaitDrained waits until no lease, dial or close is outstanding, or
// until ctx ends.
func (p *SessionPool) awaitDrained(ctx context.Context) error {
	for {
		p.mu.Lock()
		outstanding := p.leased > 0 || p.dialing > 0 || p.closing > 0
		wait := p.changed
		p.mu.Unlock()
		if !outstanding {
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
	p := l.pool
	p.mu.Lock()
	defer p.mu.Unlock()
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

// Discard closes the session; it is never handed out again. For a session
// that can no longer be trusted: one that failed with an error for which
// IsRetryable holds, or whose call outlived a deadline.
func (l *Lease) Discard() {
	p := l.pool
	p.mu.Lock()
	defer p.mu.Unlock()
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

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

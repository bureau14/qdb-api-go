// Copyright (c) 2025 QuasarDB SAS
// All rights reserved.
//
// Package qdb provides an API to a QuasarDB server.
package qdb

/*
	#include <qdb/client.h>
*/
import "C"

// Session is a connected handle: what a SessionFactory produces and what a
// pool leases. It is an alias, so everything that takes a HandleType accepts
// a Session unchanged.
type Session = HandleType

// SessionFactory dials sessions from one set of options.
//
// The options are immutable, so the factory holds no state to guard: any
// number of goroutines may call NewSession at once, each call being an
// independent qdb_open. The C API's thread-safety caveats apply within one
// handle, never across handles.
//
// Example:
//
//	factory := qdb.NewSessionFactory(qdb.NewHandleOptions().
//	    WithClusterUri("qdb://localhost:2836").
//	    WithClusterPublicKeyFile("/path/to/cluster.key").
//	    WithUserSecurityFile("/path/to/user.json"))
//	s, err := factory.NewSession()
//	if err != nil {
//	    return err
//	}
//	defer s.Close()
type SessionFactory struct {
	opts *HandleOptions
}

// NewSessionFactory creates a factory dialing with opts. It cannot fail: the
// options are judged by the C API when a session is dialed, so a
// configuration mistake surfaces on the first NewSession as an error
// IsRetryable rejects.
func NewSessionFactory(opts *HandleOptions) *SessionFactory {
	return &SessionFactory{opts: opts}
}

// Options returns the options every session is dialed with.
func (f *SessionFactory) Options() *HandleOptions {
	return f.opts
}

// NewSession opens, configures, authenticates and connects one handle. The
// caller owns the session and closes it; the factory keeps no reference.
func (f *SessionFactory) NewSession() (Session, error) {
	h, err := NewHandle()
	if err != nil {
		return HandleType{}, err
	}

	err = dialSession(h, f.opts)
	if err != nil {
		closeErr := h.Close()
		if closeErr != nil {
			L().Debug("failed to close handle during cleanup", "error", closeErr)
		}

		return HandleType{}, err
	}

	return h, nil
}

// dialSession runs the setup in the order the C API requires: every option
// before Connect, because parallelism and the per-address limit are
// captured when the handle's pools are created at connect time.
func dialSession(h HandleType, o *HandleOptions) error {
	err := setSessionTransport(h, o)
	if err != nil {
		return err
	}
	err = setSessionLimits(h, o)
	if err != nil {
		return err
	}
	err = authenticateSession(h, o)
	if err != nil {
		return err
	}

	return h.Connect(o.clusterURI)
}

// setSessionTransport applies compression, encryption and the socket
// timeout.
func setSessionTransport(h HandleType, o *HandleOptions) error {
	err := h.SetCompression(o.compression)
	if err != nil {
		return err
	}
	err = h.SetEncryption(o.encryption)
	if err != nil {
		return err
	}

	return h.SetTimeout(o.timeout)
}

// setSessionLimits applies the per-handle sizing knobs. Zero leaves the C
// API default in place. The C parameters are unsigned: a negative Go int
// would wrap to a huge value the C API would accept, so it is rejected here
// instead.
func setSessionLimits(h HandleType, o *HandleOptions) error {
	if o.clientMaxParallelism < 0 {
		return wrapError(C.qdb_e_invalid_argument, "set_client_max_parallelism", "value", o.clientMaxParallelism, "reason", "negative")
	}
	if o.clientMaxParallelism > 0 {
		err := h.SetClientMaxParallelism(uint(o.clientMaxParallelism))
		if err != nil {
			return err
		}
	}
	if o.clientMaxInBufSize > 0 {
		err := h.SetClientMaxInBufSize(o.clientMaxInBufSize)
		if err != nil {
			return err
		}
	}
	if o.connectionsPerAddress < 0 {
		return wrapError(C.qdb_e_invalid_argument, "set_connections_per_address", "value", o.connectionsPerAddress, "reason", "negative")
	}
	if o.connectionsPerAddress > 0 {
		return h.SetConnectionsPerAddress(uint(o.connectionsPerAddress))
	}

	return nil
}

// authenticateSession hands the cluster key and the user to the handle.
//
// When both come from files the C API reads them itself, at this point and
// not earlier, so the secret never passes through Go memory. The C loader
// takes both paths or none, so a mixed configuration (typically a cluster
// key file with per-user inline credentials) sets each half on its own,
// with the file half read here; still at dial time, never at construction.
func authenticateSession(h HandleType, o *HandleOptions) error {
	if o.clusterPublicKeyFile != "" && o.userSecurityFile != "" {
		return h.LoadSecurityFiles(o.clusterPublicKeyFile, o.userSecurityFile)
	}
	err := setClusterPublicKey(h, o)
	if err != nil {
		return err
	}

	return setUserCredentials(h, o)
}

// setClusterPublicKey applies the cluster key half, from its file or
// inline. A file that cannot be read is a configuration mistake and is
// reported as ErrInvalidArgument, which IsRetryable rejects, at the cost of
// errors.Is against the os error.
func setClusterPublicKey(h HandleType, o *HandleOptions) error {
	switch {
	case o.clusterPublicKeyFile != "":
		key, err := ClusterKeyFromFile(o.clusterPublicKeyFile)
		if err != nil {
			return wrapError(C.qdb_e_invalid_argument, "load_cluster_public_key", "file", o.clusterPublicKeyFile, "error", err)
		}

		return h.AddClusterPublicKey(key)
	case o.clusterPublicKey != "":
		return h.AddClusterPublicKey(o.clusterPublicKey)
	}

	return nil
}

// setUserCredentials applies the user half, from its file or inline; a
// partially set inline pair is passed on for the C API to judge. File
// errors are reported as for the cluster key.
func setUserCredentials(h HandleType, o *HandleOptions) error {
	switch {
	case o.userSecurityFile != "":
		name, secret, err := UserCredentialFromFile(o.userSecurityFile)
		if err != nil {
			return wrapError(C.qdb_e_invalid_argument, "load_user_credentials", "file", o.userSecurityFile, "error", err)
		}

		return h.AddUserCredentials(name, secret)
	case o.userName != "" || o.userSecret != "":
		return h.AddUserCredentials(o.userName, o.userSecret)
	}

	return nil
}

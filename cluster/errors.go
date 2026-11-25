// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package cluster

import "errors"

var (
	// ErrInvalidConfig is wrapped by every error that Config.Validate returns.
	ErrInvalidConfig = errors.New("cluster: invalid config")
	// ErrAlreadyJoined is returned when a System joins a cluster a second time.
	ErrAlreadyJoined = errors.New("cluster: already joined")
	// ErrNodeDown is wrapped by the stop reason of a watch whose actor's node went down.
	ErrNodeDown = errors.New("cluster: node down")
	// ErrLeft is returned for a request made after this node left the cluster.
	ErrLeft = errors.New("cluster: left")
	// ErrNotJoined is returned for a message to a grain sent before this node joined the cluster,
	// or through the zero Grain.
	ErrNotJoined = errors.New("cluster: not joined")
	// ErrNoOwner is returned for a message to a grain whose kind no up member hosts.
	ErrNoOwner = errors.New("cluster: no owner")
	// ErrInvalidKey is returned for a message to a grain whose key is not 1 to 256 bytes long.
	ErrInvalidKey = errors.New("cluster: invalid key")
)

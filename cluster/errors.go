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
)

// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remoting

// The names and help texts of the remote series (OBS-150, REM-395), in the order package remote
// registers them. The core only calls the functions it is given.
const (
	MetricFramesSent = "lockstep_remote_frames_sent_total"
	MetricFramesRecv = "lockstep_remote_frames_received_total"
	MetricBytesSent  = "lockstep_remote_bytes_sent_total"
	MetricBytesRecv  = "lockstep_remote_bytes_received_total"
	MetricReconnects = "lockstep_remote_reconnects_total"
	MetricGaps       = "lockstep_remote_gaps_total"
	MetricSessions   = "lockstep_remote_sessions"

	HelpFramesSent = "Remoting frames written to peers."
	HelpFramesRecv = "Remoting frames received from peers."
	HelpBytesSent  = "Remoting bytes written to peers, length prefixes included."
	HelpBytesRecv  = "Remoting bytes received from peers, length prefixes included."
	HelpReconnects = "Remoting sessions that came up to a peer that had one before."
	HelpGaps       = "Remoting sequence gaps seen on incoming sessions."
	HelpSessions   = "Open remoting sessions."
)

// Metrics are the counters a Core increments, each a function that adds n to one series. A nil
// function means the series is off, so each event costs one nil check. The zero Metrics counts
// nothing.
type Metrics struct {
	FramesSent     func(n uint64)
	FramesReceived func(n uint64)
	BytesSent      func(n uint64)
	BytesReceived  func(n uint64)
	Reconnects     func(n uint64)
	Gaps           func(n uint64)
}

// sent counts one frame of size bytes (length prefix included) that a link took.
func (c *Core) sent(size int) {
	if f := c.met.FramesSent; f != nil {
		f(1)
	}
	if f := c.met.BytesSent; f != nil {
		f(uint64(size)) //nolint:gosec // G115: a frame length is not negative
	}
}

// received counts one frame of size bytes (length prefix included) that arrived on a known link.
func (c *Core) received(size int) {
	if f := c.met.FramesReceived; f != nil {
		f(1)
	}
	if f := c.met.BytesReceived; f != nil {
		f(uint64(size)) //nolint:gosec // G115: a frame length is not negative
	}
}

// countUp counts s, which has just come up: it joins the open sessions, and it is a reconnect when
// a session of the same direction was up to its peer before.
func (c *Core) countUp(s *session) {
	s.counted = true
	c.up.Add(1)
	seen := &s.peer.upIn
	if s.dir == dirOut {
		seen = &s.peer.upOut
	}
	if *seen && c.met.Reconnects != nil {
		c.met.Reconnects(1)
	}
	*seen = true
}

// Sessions returns the number of sessions that are up, for the sessions gauge. It is safe for
// concurrent use and takes no lock.
func (c *Core) Sessions() int64 { return c.up.Load() }

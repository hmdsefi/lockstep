// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"fmt"
	"strconv"

	"github.com/hmdsefi/lockstep/internal/mailbox"
)

// MailboxPolicy selects what happens when a message is sent to a full mailbox. A mailbox is full
// when it holds as many sent messages as its capacity. Timer messages, piped replies and effect
// results are never refused and never count toward capacity.
type MailboxPolicy uint8

// The full-mailbox policies. Their values and order are those of mailbox.Policy.
const (
	// FailSend refuses the message: Send returns an error wrapping ErrMailboxFull. The default.
	FailSend MailboxPolicy = iota
	// DropNewest drops the message being sent and records the drop. Send returns nil.
	DropNewest
	// DropOldest drops the oldest sent message in the mailbox, records the drop and enqueues the
	// new one. Send returns nil.
	DropOldest
	// BlockSender makes an outside sender (a *System) wait until the mailbox has room and the
	// System admits messages again, or until its context ends. For a sender inside an actor, and
	// on engines that cannot make a caller wait (simulation), it behaves as FailSend.
	BlockSender
)

// String returns "fail-send", "drop-newest", "drop-oldest", "block-sender", or
// "MailboxPolicy(<n>)" for other values. Records, logs and metrics use these spellings (OBS-145).
func (p MailboxPolicy) String() string {
	switch p {
	case FailSend:
		return "fail-send"
	case DropNewest:
		return "drop-newest"
	case DropOldest:
		return "drop-oldest"
	case BlockSender:
		return "block-sender"
	}
	return "MailboxPolicy(" + strconv.Itoa(int(p)) + ")"
}

// WithMailbox sets the spawned actor's mailbox capacity and full-mailbox policy. It never
// panics: if capacity < 1 or policy is not one of the four constants, Spawn returns an error
// wrapping ErrInvalidOption. Of several valid WithMailbox options the last one counts.
func WithMailbox(capacity int, policy MailboxPolicy) SpawnOption {
	return func(c *spawnConfig) {
		m := mailboxConfig{capacity, policy}
		if err := m.validate(); err != nil {
			c.fail(err)
			return
		}
		c.mbx, c.mbxSet = m, true
	}
}

// WithDefaultMailbox sets the capacity and policy of every actor spawned in the system without
// WithMailbox. Without it the default is 1024 and FailSend. It never panics: an invalid capacity
// or policy makes New panic with an error wrapping ErrInvalidOption. Of several valid
// WithDefaultMailbox options the last one counts.
func WithDefaultMailbox(capacity int, policy MailboxPolicy) Option {
	return func(c *systemConfig) {
		m := mailboxConfig{capacity, policy}
		if err := m.validate(); err != nil {
			c.fail(err)
			return
		}
		c.mbx = m
	}
}

// mailboxConfig is the capacity and policy of one mailbox (MBX-010).
type mailboxConfig struct {
	capacity int
	policy   MailboxPolicy
}

// defaultMailbox is the mailbox of an actor spawned without WithMailbox in a system built
// without WithDefaultMailbox (MBX-010).
var defaultMailbox = mailboxConfig{capacity: mailbox.DefaultCapacity, policy: FailSend}

// validate returns the error of MBX-020 for an invalid capacity or policy, and nil otherwise.
func (m mailboxConfig) validate() error {
	if m.capacity < 1 {
		return fmt.Errorf("%w: mailbox capacity must be at least 1, got %d", ErrInvalidOption, m.capacity)
	}
	if m.policy > BlockSender {
		return fmt.Errorf("%w: unknown mailbox policy %s", ErrInvalidOption, m.policy)
	}
	return nil
}

// queuePolicy converts p, which validate accepted, to the queue's policy.
func (p MailboxPolicy) queuePolicy() mailbox.Policy { return mailbox.Policy(p) }

// entryKind tells how an entry reached the user mailbox (MBX §11). It maps to mailbox.Kind.
type entryKind uint8

const (
	entrySent entryKind = iota + 1 // Send or Ask: counts against capacity
	entrySelf                      // a piped reply, timer message or effect result of incarnation inc
)

// entryKindOf returns the entry kind of the queue's kind k.
func entryKindOf(k mailbox.Kind) entryKind {
	if k == mailbox.Self {
		return entrySelf
	}
	return entrySent
}

// queueKind returns the queue's kind of k: only a Sent entry counts against capacity.
func (k entryKind) queueKind() mailbox.Kind {
	if k == entrySelf {
		return mailbox.Self
	}
	return mailbox.Sent
}

// entry is one user-mailbox element (MBX §11).
type entry struct {
	msg  any     // the M
	from Address // the sender
	ask  Address // MBX-092's ask address of a request, zero otherwise; Ask sets it
	seq  uint64  //nolint:unused // the inbound envelope's Seq for MBX §8's deliver record; 0 for a local send
	inc  uint32  // entrySelf's incarnation (MBX-170); Pipe sets it
	kind entryKind
}

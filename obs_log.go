// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package lockstep

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/hmdsefi/lockstep/internal/prodengine"
	"github.com/hmdsefi/lockstep/internal/supervise"
)

// The runtime log events of OBS-060. Each event has one helper, which its owner calls once per
// occurrence at its decision point, holding no lockstep lock (a handler is user code). Only a
// system New built logs them: on any other system, simulation's included, events is nil and
// every helper returns after one nil check (OBS-155). A helper asks Enabled before it builds an
// attribute, so a disabled level allocates nothing (OBS-070). Actor events carry node, path and
// uid first, system events node first (OBS-020); the actor attributes are passed explicitly
// rather than through the actor's logger, which Spawn never builds (OBS-030).

// The reason attribute of message dropped (OBS-145).
const (
	dropNewest          = "drop-newest"
	dropOldest          = "drop-oldest"
	dropLateReply       = "late-reply"
	dropCancelledEffect = "cancelled-effect"
)

// eventLog returns the logger for an event at level, or nil when the event is not logged: the
// system was not built by New, a test removed the logging calls (logOff, AT-OBS-06), or the
// logger is not enabled for level. It allocates nothing.
func (s *System) eventLog(level slog.Level) *slog.Logger {
	l := s.events
	if l == nil || s.logOff.Load() || !l.Enabled(context.Background(), level) {
		return nil
	}
	return l
}

// logActorEvent logs an actor event at level with the actor attributes of a, then attrs. The caller
// got l from eventLog.
func logActorEvent(l *slog.Logger, level slog.Level, msg string, a Address, attrs ...slog.Attr) {
	all := make([]slog.Attr, 0, 3+len(attrs))
	all = append(all, slog.String("node", a.node), slog.String("path", a.path), slog.Uint64("uid", a.uid))
	all = append(all, attrs...)
	l.LogAttrs(context.Background(), level, msg, all...)
}

// logSystemEvent logs a system event at level with the node attribute, then attrs.
func (s *System) logSystemEvent(l *slog.Logger, level slog.Level, msg string, attrs ...slog.Attr) {
	all := make([]slog.Attr, 0, 1+len(attrs))
	all = append(all, slog.String("node", s.node))
	all = append(all, attrs...)
	l.LogAttrs(context.Background(), level, msg, all...)
}

// logActorFailed logs actor failed for the failure err of the actor a, which its supervisor
// decided with the final directive d (SUP-180); src is the failure's source. The failure is a
// panic when err is the *PanicError of a panic in a's own handler, not one an escalation carried
// up from a child (SUP-220).
func (s *System) logActorFailed(a Address, err error, src supervise.Source, d supervise.Directive) {
	l := s.eventLog(slog.LevelError)
	if l == nil {
		return
	}
	_, panicked := err.(*PanicError)
	panicked = panicked && src != supervise.InEscalate
	logActorEvent(l, slog.LevelError, "actor failed", a,
		slog.Any("err", err),
		slog.String("directive", Directive(d).String()),
		slog.Bool("panic", panicked),
	)
}

// logRestartIntensity logs restart intensity exceeded for the actor a, whose restart one more
// restart within limits' window would have exceeded limits' MaxRestarts (SUP-270).
func (s *System) logRestartIntensity(a Address, limits supervise.Limits) {
	l := s.eventLog(slog.LevelError)
	if l == nil {
		return
	}
	logActorEvent(l, slog.LevelError, "restart intensity exceeded", a,
		slog.Int("max_restarts", limits.MaxRestarts),
		slog.Duration("window", limits.Window),
	)
}

// logActorRestarted logs actor restarted for the actor a, whose incarnation ended for a restart
// caused by err (SUP-190), where lockstep.restart is recorded.
func (s *System) logActorRestarted(a Address, err error) {
	l := s.eventLog(slog.LevelInfo)
	if l == nil {
		return
	}
	logActorEvent(l, slog.LevelInfo, "actor restarted", a, slog.Any("err", err))
}

// logStoppedPanic logs stopped hook panicked for the actor a, whose Stopped hook panicked with
// pe (SUP-110).
func (s *System) logStoppedPanic(a Address, pe *PanicError) {
	l := s.eventLog(slog.LevelError)
	if l == nil {
		return
	}
	logActorEvent(l, slog.LevelError, "stopped hook panicked", a,
		slog.String("panic", fmt.Sprint(pe.Value)),
		slog.String("stack", string(pe.Stack)),
	)
}

// logCloseFailed logs resource close failed for the resource the actor a registered at index,
// whose Close returned or panicked with err (SUP-120).
func (s *System) logCloseFailed(a Address, index int, err error) {
	l := s.eventLog(slog.LevelWarn)
	if l == nil {
		return
	}
	logActorEvent(l, slog.LevelWarn, "resource close failed", a, slog.Any("err", err), slog.Int("index", index))
}

// logEffectPanicked logs effect panicked for an effect of the actor a whose outcome is err, if
// its Run panicked: the production engine reports that with a *prodengine.EffectPanicError
// (SCH-180).
func (s *System) logEffectPanicked(a Address, err error) {
	l := s.eventLog(slog.LevelError)
	if l == nil {
		return
	}
	var pe *prodengine.EffectPanicError
	if !errors.As(err, &pe) {
		return
	}
	logActorEvent(l, slog.LevelError, "effect panicked", a,
		slog.String("effect", pe.Name),
		slog.String("panic", fmt.Sprint(pe.Value)),
		slog.String("stack", string(pe.Stack)),
	)
}

// logMailboxFull logs mailbox full for a send that found the mailbox of c, the actor at owner,
// full (MBX-090): once per send, a BlockSender send when it starts to wait.
func (s *System) logMailboxFull(c actorCell, owner Address) {
	l := s.eventLog(slog.LevelDebug)
	if l == nil {
		return
	}
	_, _, policy := c.mailboxState()
	logActorEvent(l, slog.LevelDebug, "mailbox full", owner, slog.String("policy", policy.String()))
}

// logMessageShed logs message shed for a message to c, the actor at owner, that its mailbox
// treated as full because admission refused it with err, or c is stuck (OVL-120, OVL-520).
func (s *System) logMessageShed(c actorCell, owner Address, err error) {
	l := s.eventLog(slog.LevelDebug)
	if l == nil {
		return
	}
	_, _, policy := c.mailboxState()
	logActorEvent(l, slog.LevelDebug, "message shed", owner,
		slog.String("policy", policy.String()),
		slog.String("reason", overloadReason(err)),
	)
}

// logMessageDropped logs message dropped with reason for a message or result discarded on its
// way to a: a mailbox's drop policy, a reply to an ask that resolved already (a is the ask
// address), or the result of an effect a stop or restart cancelled.
func (s *System) logMessageDropped(a Address, reason string) {
	l := s.eventLog(slog.LevelDebug)
	if l == nil {
		return
	}
	logActorEvent(l, slog.LevelDebug, "message dropped", a, slog.String("reason", reason))
}

// logDeadLetter logs dead letter for msg, sent to to, which went to dead letters with reason
// (MBX-150).
func (s *System) logDeadLetter(to Address, reason string, msg any) {
	l := s.eventLog(slog.LevelDebug)
	if l == nil {
		return
	}
	s.logSystemEvent(l, slog.LevelDebug, "dead letter",
		slog.String("to", to.String()),
		slog.String("reason", reason),
		slog.String("type", fmt.Sprintf("%T", msg)),
	)
}

// logShutdownStarted logs shutdown started with the number of live actors (ACT-050).
func (s *System) logShutdownStarted() {
	l := s.eventLog(slog.LevelInfo)
	if l == nil {
		return
	}
	s.logSystemEvent(l, slog.LevelInfo, "shutdown started", slog.Int("actors", s.liveActors()))
}

// logShutdownFinished logs shutdown finished with the time since the shutdown started, on the
// engine clock (ACT-050).
func (s *System) logShutdownFinished(elapsed time.Duration) {
	l := s.eventLog(slog.LevelInfo)
	if l == nil {
		return
	}
	s.logSystemEvent(l, slog.LevelInfo, "shutdown finished", slog.Duration("elapsed", elapsed))
}

// logShutdownIncomplete logs shutdown incomplete for a Shutdown call whose context ended with err
// before the system stopped (ACT-060).
func (s *System) logShutdownIncomplete(err error) {
	l := s.eventLog(slog.LevelError)
	if l == nil {
		return
	}
	s.logSystemEvent(l, slog.LevelError, "shutdown incomplete", slog.Any("err", err))
}

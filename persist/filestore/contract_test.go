// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package filestore

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/hmdsefi/lockstep/persist"
)

// contract is the scenario of the store contract table (AT-PER-22). Strict, every expectation
// is checked; lenient, as when the FS fails on purpose (AT-PER-30), the calls are made and the
// expectations are ignored. In both, every call of the eight methods goes through a wrapper that
// checks that no file is left open when the call returns.
type contract struct {
	t      *testing.T
	s      *Store
	r      *recFS
	dir    string
	strict bool
}

func newContract(t *testing.T, strict bool, opts ...Option) *contract {
	t.Helper()
	s, r, dir := newJournal(t, opts...)
	return &contract{t: t, s: s, r: r, dir: dir, strict: strict}
}

// expect reports a failed expectation, in strict mode only.
func (c *contract) expect(ok bool, format string, args ...any) {
	c.t.Helper()
	if c.strict && !ok {
		c.t.Errorf(format, args...)
	}
}

// expectErr expects err to wrap target; a nil target expects no error.
func (c *contract) expectErr(what string, err, target error) {
	c.t.Helper()
	if target == nil {
		c.expect(err == nil, "%s: %v, want success", what, err)
		return
	}
	c.expect(errors.Is(err, target), "%s: %v, want an error wrapping %v", what, err, target)
}

// after runs after every call: no file may be open.
func (c *contract) after() {
	c.t.Helper()
	c.r.checkClosed(c.t)
}

func (c *contract) recoverID(ctx context.Context, id string) persist.Bounds { //nolint:unparam // the scenarios use one ID
	c.t.Helper()
	b, err := c.s.Recover(ctx, id)
	c.after()
	c.expectErr("Recover", err, nil)
	return b
}

func (c *contract) write(ctx context.Context, id string, events []persist.Event) error {
	c.t.Helper()
	err := c.s.Write(ctx, id, events)
	c.after()
	return err
}

func (c *contract) commit(ctx context.Context, id string) error {
	c.t.Helper()
	err := c.s.Commit(ctx, id)
	c.after()
	return err
}

func (c *contract) read(ctx context.Context, id string, from uint64, max int) ([]persist.Event, error) { //nolint:unparam // the scenarios use one ID
	c.t.Helper()
	events, err := c.s.Read(ctx, id, from, max)
	c.after()
	return events, err
}

func (c *contract) deleteEvents(ctx context.Context, id string, to uint64) error {
	c.t.Helper()
	err := c.s.DeleteEvents(ctx, id, to)
	c.after()
	return err
}

func (c *contract) save(ctx context.Context, id string, seq uint64) error { //nolint:unparam // the scenarios use one ID
	c.t.Helper()
	err := c.s.Save(ctx, id, snap(seq))
	c.after()
	return err
}

func (c *contract) load(ctx context.Context, id string) (persist.Snapshot, bool, error) { //nolint:unparam // the scenarios use one ID
	c.t.Helper()
	sn, ok, err := c.s.Load(ctx, id)
	c.after()
	return sn, ok, err
}

func (c *contract) deleteSnapshots(ctx context.Context, id string, to uint64) error {
	c.t.Helper()
	err := c.s.DeleteSnapshots(ctx, id, to)
	c.after()
	return err
}

// writeCommit writes events and commits them.
func (c *contract) writeCommit(id string, events []persist.Event) { //nolint:unparam // the scenarios use one ID
	c.t.Helper()
	ctx := context.Background()
	if err := c.write(ctx, id, events); err == nil {
		_ = c.commit(ctx, id)
	}
}

// tree returns the content of every file below the store root, by name; directories map to nil.
func (c *contract) tree() map[string][]byte {
	c.t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(c.dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(c.dir, p)
		if err != nil {
			return err
		}
		if d.IsDir() {
			out[rel] = nil
			return nil
		}
		b, err := os.ReadFile(p) //nolint:gosec // a test reads the files of its own temporary directory
		out[rel] = b
		return err
	})
	if err != nil {
		c.t.Fatal(err)
	}
	return out
}

// contractStep is one item of the contract table.
type contractStep struct {
	name string
	run  func(c *contract)
}

var contractSteps = []contractStep{
	{"first accepted seq is 1", func(c *contract) {
		ctx := context.Background()
		b := c.recoverID(ctx, "c-1")
		c.expect(b == persist.Bounds{First: 1}, "Recover = %+v, want {1 0 0}", b)
		c.expectErr("Write(seq 2)", c.write(ctx, "c-1", ev(2, "x")), persist.ErrSeqConflict)
		c.expectErr("Write(seq 0)", c.write(ctx, "c-1", ev(0, "x")), persist.ErrSeqConflict)
		c.expectErr("Write(seq 1)", c.write(ctx, "c-1", ev(1, "x")), nil)
	}},
	{"a sequence conflict writes nothing", func(c *contract) {
		ctx := context.Background()
		c.recoverID(ctx, "c-1")
		c.writeCommit("c-1", ev(1, "e1"))
		b := c.recoverID(ctx, "c-1")
		c.expect(b == persist.Bounds{First: 1, Last: 1}, "Recover = %+v, want {1 1 0}", b)
		before := c.tree()
		c.expectErr("Write(seq 3)", c.write(ctx, "c-1", ev(3, "e3")), persist.ErrSeqConflict)
		c.expectErr("Write(seq 2, 4)", c.write(ctx, "c-1", append(ev(2, "e2"), ev(4, "e4")...)), persist.ErrSeqConflict)
		c.expect(reflect.DeepEqual(before, c.tree()), "a rejected Write changed the files")
		c.expectErr("Write(seq 2)", c.write(ctx, "c-1", ev(2, "e2")), nil)
	}},
	{"an empty batch is invalid", func(c *contract) {
		ctx := context.Background()
		c.recoverID(ctx, "c-1")
		before := c.tree()
		c.expectErr("Write(nil)", c.write(ctx, "c-1", nil), fs.ErrInvalid)
		c.expectErr("Write([])", c.write(ctx, "c-1", []persist.Event{}), fs.ErrInvalid)
		c.expect(reflect.DeepEqual(before, c.tree()), "an empty Write changed the files")
	}},
	{"read below F is missing events", func(c *contract) {
		ctx := context.Background()
		c.recoverID(ctx, "c-1")
		for i := uint64(1); i <= 3; i++ {
			c.writeCommit("c-1", ev(i, fmt.Sprintf("e%d", i)))
		}
		c.expectErr("DeleteEvents(2)", c.deleteEvents(ctx, "c-1", 2), nil)
		b := c.recoverID(ctx, "c-1")
		c.expect(b.First == 3 && b.Last == 3, "Recover = %+v, want First 3, Last 3", b)
		_, err := c.read(ctx, "c-1", 1, 10)
		c.expectErr("Read(1)", err, persist.ErrMissingEvents)
		got, err := c.read(ctx, "c-1", 3, 10)
		c.expectErr("Read(3)", err, nil)
		c.expect(fmt.Sprint(seqsOf(got)) == "[3]", "Read(3) = %v, want [3]", seqsOf(got))
	}},
	{"read past L is empty", func(c *contract) {
		ctx := context.Background()
		c.recoverID(ctx, "c-1")
		c.writeCommit("c-1", ev(1, "e1", "e2"))
		got, err := c.read(ctx, "c-1", 3, 10)
		c.expectErr("Read(3)", err, nil)
		c.expect(got == nil, "Read(3) = %v, want nil", got)
	}},
	{"uncommitted events are not returned", func(c *contract) {
		ctx := context.Background()
		c.recoverID(ctx, "c-1")
		c.writeCommit("c-1", ev(1, "e1"))
		c.expectErr("Write(2)", c.write(ctx, "c-1", ev(2, "e2")), nil)
		got, err := c.read(ctx, "c-1", 1, 10)
		c.expectErr("Read(1)", err, nil)
		c.expect(fmt.Sprint(seqsOf(got)) == "[1]", "Read(1) = %v, want [1]", seqsOf(got))
		got, err = c.read(ctx, "c-1", 2, 10)
		c.expectErr("Read(2)", err, nil)
		c.expect(got == nil, "Read(2) = %v, want nil", got)
	}},
	{"DeleteEvents leaves Last", func(c *contract) {
		ctx := context.Background()
		c.recoverID(ctx, "c-1")
		for i := uint64(1); i <= 4; i++ {
			c.writeCommit("c-1", ev(i, fmt.Sprintf("e%d", i)))
		}
		c.expectErr("DeleteEvents(100)", c.deleteEvents(ctx, "c-1", 100), nil)
		b := c.recoverID(ctx, "c-1")
		c.expect(b.Last == 4 && b.First >= 1 && b.First <= 5, "Recover = %+v, want Last 4", b)
		c.expectErr("Write(5)", c.write(ctx, "c-1", ev(5, "e5")), nil)
	}},
	{"slices belong to the caller", func(c *contract) {
		ctx := context.Background()
		c.recoverID(ctx, "c-1")
		data := []byte("abc")
		c.writeCommit("c-1", []persist.Event{{Seq: 1, Data: data}})
		data[0] = 'X' // after Write
		got, err := c.read(ctx, "c-1", 1, 1)
		c.expectErr("Read(1)", err, nil)
		c.expect(len(got) == 1 && string(got[0].Data) == "abc", "Read(1) = %+v after the caller changed the slice", got)
		if len(got) == 1 {
			got[0].Data[0] = 'Y' // after Read
		}
		got, err = c.read(ctx, "c-1", 1, 1)
		c.expectErr("Read(1)", err, nil)
		c.expect(len(got) == 1 && string(got[0].Data) == "abc", "Read(1) = %+v after the caller changed a result", got)

		sdata := []byte("snap")
		c.expectErr("Save", c.s.Save(ctx, "c-1", persist.Snapshot{Seq: 1, Data: sdata}), nil)
		sdata[0] = 'X'
		sn, ok, err := c.load(ctx, "c-1")
		c.expectErr("Load", err, nil)
		c.expect(ok && string(sn.Data) == "snap", "Load = %+v, %v after the caller changed the slice", sn, ok)
	}},
	{"a done ctx changes nothing", func(c *contract) {
		bg := context.Background()
		c.recoverID(bg, "c-1")
		c.writeCommit("c-1", ev(1, "e1", "e2"))
		_ = c.save(bg, "c-1", 1)
		_ = c.save(bg, "c-1", 2)
		before := c.tree()
		m := c.r.mark()
		ctx, cancel := context.WithCancel(bg)
		cancel()
		_, rerr := c.s.Recover(ctx, "c-1")
		c.after()
		c.expectErr("Recover", rerr, context.Canceled)
		_, err := c.read(ctx, "c-1", 1, 1)
		c.expectErr("Read", err, context.Canceled)
		c.expectErr("Write", c.write(ctx, "c-1", ev(3, "e3")), context.Canceled)
		c.expectErr("Commit", c.commit(ctx, "c-1"), context.Canceled)
		c.expectErr("DeleteEvents", c.deleteEvents(ctx, "c-1", 1), context.Canceled)
		c.expectErr("Save", c.save(ctx, "c-1", 3), context.Canceled)
		_, _, err = c.load(ctx, "c-1")
		c.expectErr("Load", err, context.Canceled)
		c.expectErr("DeleteSnapshots", c.deleteSnapshots(ctx, "c-1", 2), context.Canceled)
		c.expect(reflect.DeepEqual(before, c.tree()), "a call with a done ctx changed the files")
		c.expect(len(c.r.since(m)) == 0, "a call with a done ctx used the FS: %q", c.r.since(m))
	}},
	{"Load returns the newer of two snapshots", func(c *contract) {
		ctx := context.Background()
		_ = c.save(ctx, "c-1", 2)
		_ = c.save(ctx, "c-1", 5)
		sn, ok, err := c.load(ctx, "c-1")
		c.expectErr("Load", err, nil)
		c.expect(ok && sn.Seq == 5 && string(sn.Data) == "s5", "Load = %+v, %v, want snapshot 5", sn, ok)
		c.expectErr("DeleteSnapshots(5)", c.deleteSnapshots(ctx, "c-1", 5), nil)
		_, ok, err = c.load(ctx, "c-1")
		c.expectErr("Load", err, nil)
		c.expect(!ok, "Load found a snapshot after all were deleted")
	}},
	{"8 goroutines write 8 IDs", func(c *contract) {
		const ids, rounds = 8, 6
		var wg sync.WaitGroup
		for i := range ids {
			wg.Add(1)
			go func() {
				defer wg.Done()
				id := fmt.Sprintf("c-%d", i)
				ctx := context.Background()
				ok := func(op string, err error) {
					if c.strict && err != nil {
						c.t.Errorf("%s %s: %v", id, op, err)
					}
				}
				_, err := c.s.Recover(ctx, id)
				ok("Recover", err)
				for seq := uint64(1); seq <= rounds; seq++ {
					ok("Write", c.s.Write(ctx, id, ev(seq, id)))
					ok("Commit", c.s.Commit(ctx, id))
					if seq%2 == 0 {
						ok("Save", c.s.Save(ctx, id, snap(seq)))
						ok("DeleteSnapshots", c.s.DeleteSnapshots(ctx, id, seq-2))
						ok("DeleteEvents", c.s.DeleteEvents(ctx, id, seq-1))
					}
					got, err := c.s.Read(ctx, id, seq, 1)
					ok("Read", err)
					if c.strict && err == nil && (len(got) != 1 || got[0].Seq != seq || string(got[0].Data) != id) {
						c.t.Errorf("%s Read(%d) = %+v", id, seq, got)
					}
					sn, found, err := c.s.Load(ctx, id)
					ok("Load", err)
					if c.strict && err == nil && seq >= 2 && (!found || sn.Seq != seq-seq%2) {
						c.t.Errorf("%s Load = %+v, %v after seq %d", id, sn, found, seq)
					}
				}
			}()
		}
		wg.Wait()
		c.r.checkClosed(c.t)
		if c.strict {
			for i := range ids {
				s, err := Open(c.dir)
				if err != nil {
					c.t.Fatal(err)
				}
				if b, err := s.Recover(context.Background(), fmt.Sprintf("c-%d", i)); err != nil || b.Last != rounds {
					c.t.Errorf("c-%d: Recover = %+v, %v, want Last %d", i, b, err, rounds)
				}
			}
		}
	}},
}

// AT-PER-22
func TestATPER22StoreContract(t *testing.T) {
	for _, step := range contractSteps {
		t.Run(step.name, func(t *testing.T) {
			step.run(newContract(t, true, WithSegmentSize(1)))
		})
	}
}

// AT-PER-30: the scenario of AT-PER-22 with an FS that fails every third WriteAt and every
// second Sync leaves no file open after any call.
func TestATPER30NoFileOutlivesCall(t *testing.T) {
	errWrite, errSync := errors.New("injected write"), errors.New("injected sync")
	failedWrite, failedSync := 0, 0
	for _, step := range contractSteps {
		t.Run(step.name, func(t *testing.T) {
			c := newContract(t, false, WithSegmentSize(1))
			c.r.fail("WriteAt", "", every(3), &partialWrite{n: 9, err: errWrite})
			c.r.fail("Sync", "", every(2), errSync)
			step.run(c)
			c.r.checkClosed(t)
			for _, rule := range c.r.rules {
				switch rule.err.(type) {
				case *partialWrite:
					failedWrite += rule.n / 3
				default:
					failedSync += rule.n / 2
				}
			}
		})
	}
	if failedWrite == 0 || failedSync == 0 {
		t.Fatalf("injected %d WriteAt and %d Sync failures, want some of each", failedWrite, failedSync)
	}
}

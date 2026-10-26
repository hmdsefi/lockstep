// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim

import (
	"cmp"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"time"

	"github.com/hmdsefi/faultline/kernel/simdisk"
	"github.com/hmdsefi/lockstep/internal/engine"
	"github.com/hmdsefi/lockstep/persist/filestore"
)

// execution is the node event sim runs at the moment, as the Volume adapter sees it (PER-510,
// PER-540): the engine of its node, the actor whose turn it runs and the inline effect run in
// progress. Tracking it adds no record, no event and no random draw.
type execution struct {
	eng   *simEngine     // the engine whose node's event runs; nil outside node events
	actor engine.Address // the actor whose turn runs; the zero Address when no turn runs
	run   *inlineRun     // the inline effect run in progress, or nil
}

// enter makes x the execution in progress and returns a function that restores the previous one.
// Kernel events never nest, but a boot can run inside another node's event (a Restart), so the
// previous execution is restored, not cleared.
func (s *Sim) enter(x execution) (leave func()) {
	prev := s.exec
	s.exec = x
	return func() { s.exec = prev }
}

// owner is the owner of a file opened through a Volume (PER-540): the actor incarnation whose
// turn or inline effect run opened it, or the node (the zero owner) when no actor code ran.
type owner struct {
	actor string // the actor's address text, as lockstep.start records it; "" for the node
	inc   string // the incarnation, as lockstep.start records it
}

// inlineRun is one run of an inline internal effect (PER-520): the incarnation that started it,
// which owns the files it opens, and the disk latency its Volume calls have sampled (PER-530).
type inlineRun struct {
	owner   owner
	latency time.Duration
}

// owner returns the owner of a file opened now (PER-540): the incarnation of the inline run in
// progress, the incarnation of the actor whose turn runs, read now because a turn can begin a
// new incarnation, or the node.
func (s *Sim) owner() owner {
	x := s.exec
	if x.run != nil {
		return x.run.owner
	}
	if x.actor != (engine.Address{}) {
		return x.eng.ownerOf(x.actor.String())
	}
	return owner{}
}

// ownerOf returns the current incarnation of the actor with address text actor, from its latest
// lockstep.start record.
func (e *simEngine) ownerOf(actor string) owner {
	return owner{actor: actor, inc: e.starts[actor]}
}

// Volume returns the simulated disk of node as a filestore.FS: World().Disk.Volume of that node,
// with names mapped under "/". Every call for one node returns the same value, valid in every
// incarnation. It panics if no node or client is named node.
//
// Use the FS only from the node's own boots, turns and effects. A call from another node's event,
// or from outside any node event, fails the seed with check lockstep/misuse and returns a
// *fs.PathError wrapping fs.ErrPermission without touching the disk. A file an actor opens must
// be closed before that actor stops or restarts; otherwise the seed fails with check
// lockstep/leak. A call made by a storage operation of package persist adds the disk latency
// drawn from Options.Disk.Latency to the time that operation takes. Like the methods of s, the
// FS and its files panic once the run of s has ended.
func (s *Sim) Volume(node string) filestore.FS {
	s.check()
	ln, ok := s.byName[node]
	if !ok {
		panic(fmt.Sprintf("sim: Volume: unknown node %q", node))
	}
	if ln.vol == nil {
		ln.vol = &volume{
			s:       s,
			ln:      ln,
			vol:     s.w.Disk.Volume(ln.n),
			latency: s.w.Disk.Config().Latency != (simdisk.LatencyConfig{}),
		}
	}
	return ln.vol
}

// volume is the filestore.FS of one node's simdisk volume (PER-500).
type volume struct {
	s       *Sim
	ln      *node
	vol     *simdisk.Volume
	latency bool // Options.Disk.Latency is set: inline runs sample it (PER-530)
}

var (
	_ filestore.FS   = (*volume)(nil)
	_ filestore.File = (*volFile)(nil)
)

// allowed checks that an event of the volume's node runs (PER-510). Otherwise it stores the
// misuse failure in the lockstep/misuse slot, emits lockstep.misuse, and returns the
// *fs.PathError the method named op returns for name.
func (v *volume) allowed(op, name string) error {
	v.s.check()
	x := v.s.exec
	if x.eng != nil && x.eng.n == v.ln.n {
		return nil
	}
	if x.eng != nil {
		x.eng.misuse(fmt.Errorf("sim: volume of node %q used from node %q", v.ln.n.Name(), x.eng.n.Name()))
	} else {
		err := fmt.Errorf("sim: volume of node %q used outside a node event", v.ln.n.Name())
		v.s.emit(0, "lockstep.misuse", "misuse", []engine.Attr{{Key: "error", Value: err.Error()}})
		v.s.store(checkMisuse, err)
	}
	return &fs.PathError{Op: op, Path: name, Err: fs.ErrPermission}
}

// enter checks a call of the FS method op with name: the event (PER-510), then the name, which
// must satisfy fs.ValidPath and contain no backslash (PER-500). It samples the latency of class class and returns the
// volume path of name.
func (v *volume) enter(op, name string, class simdisk.Op) (string, error) {
	if err := v.allowed(op, name); err != nil {
		return "", err
	}
	p, err := volumePath(op, name)
	if err != nil {
		return "", err
	}
	v.sample(class)
	return p, nil
}

// volumePath maps the FS name to its volume path: "." to "/" and x to "/" + x (PER-500). A name
// failing fs.ValidPath or containing a backslash returns the error of the FS method op, as the
// OS FS does (PER-430).
func volumePath(op, name string) (string, error) {
	if !fs.ValidPath(name) || strings.ContainsRune(name, '\\') {
		return "", &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	if name == "." {
		return "/", nil
	}
	return "/" + name, nil
}

// sample adds the latency of one operation of class op to the inline run in progress (PER-530).
// Outside inline runs, and without Options.Disk.Latency, it draws nothing.
func (v *volume) sample(op simdisk.Op) {
	if r := v.s.exec.run; r != nil && v.latency {
		r.latency += v.vol.SampleLatency(op)
	}
}

// open tracks the file f just opened, owned by whoever opens it now (PER-540).
func (v *volume) open(f *simdisk.File) filestore.File {
	vf := &volFile{v: v, f: f, owner: v.s.owner()}
	v.ln.files = append(v.ln.files, vf)
	return vf
}

func (v *volume) Create(name string) (filestore.File, error) {
	p, err := v.enter("Create", name, simdisk.OpMeta)
	if err != nil {
		return nil, err
	}
	f, err := v.vol.Create(p)
	if err != nil {
		return nil, err
	}
	return v.open(f), nil
}

func (v *volume) Open(name string) (filestore.File, error) {
	p, err := v.enter("Open", name, simdisk.OpMeta)
	if err != nil {
		return nil, err
	}
	f, err := v.vol.Open(p)
	if err != nil {
		return nil, err
	}
	return v.open(f), nil
}

func (v *volume) Remove(name string) error {
	p, err := v.enter("Remove", name, simdisk.OpMeta)
	if err != nil {
		return err
	}
	return v.vol.Remove(p)
}

func (v *volume) Rename(oldname, newname string) error {
	if err := v.allowed("Rename", oldname); err != nil {
		return err
	}
	from, err := volumePath("Rename", oldname)
	if err != nil {
		return err
	}
	to, err := volumePath("Rename", newname)
	if err != nil {
		return err
	}
	v.sample(simdisk.OpMeta)
	return v.vol.Rename(from, to)
}

func (v *volume) MkdirAll(name string) error {
	p, err := v.enter("MkdirAll", name, simdisk.OpMeta)
	if err != nil {
		return err
	}
	return v.vol.MkdirAll(p)
}

func (v *volume) ReadDir(name string) ([]filestore.Info, error) {
	p, err := v.enter("ReadDir", name, simdisk.OpMeta)
	if err != nil {
		return nil, err
	}
	entries, err := v.vol.ReadDir(p) // sorted by name
	if err != nil {
		return nil, err
	}
	infos := make([]filestore.Info, len(entries))
	for i, e := range entries {
		infos[i] = filestore.Info{Name: e.Name, Size: e.Size, IsDir: e.IsDir}
	}
	return infos, nil
}

func (v *volume) SyncDir(name string) error {
	p, err := v.enter("SyncDir", name, simdisk.OpSync)
	if err != nil {
		return err
	}
	return v.vol.SyncDir(p)
}

// volFile is a filestore.File of a volume: a simdisk handle and the owner that opened it. It is
// tracked in its node's open files until its Close returns (PER-540).
type volFile struct {
	v     *volume
	f     *simdisk.File
	owner owner
}

func (f *volFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.enter("ReadAt", simdisk.OpRead); err != nil {
		return 0, err
	}
	return f.f.ReadAt(p, off)
}

func (f *volFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.enter("WriteAt", simdisk.OpWrite); err != nil {
		return 0, err
	}
	return f.f.WriteAt(p, off)
}

func (f *volFile) Truncate(size int64) error {
	if err := f.enter("Truncate", simdisk.OpWrite); err != nil {
		return err
	}
	return f.f.Truncate(size)
}

// Size returns simdisk's size, or an error wrapping fs.ErrClosed for a closed or stale handle
// (PER-500). It samples no latency (PER-530).
func (f *volFile) Size() (int64, error) {
	if err := f.v.allowed("Size", f.f.Name()); err != nil {
		return 0, err
	}
	n := f.f.Size()
	if n < 0 {
		return 0, &fs.PathError{Op: "Size", Path: f.f.Name(), Err: fs.ErrClosed}
	}
	return n, nil
}

func (f *volFile) Sync() error {
	if err := f.enter("Sync", simdisk.OpSync); err != nil {
		return err
	}
	return f.f.Sync()
}

// Close closes the handle and stops tracking it, whether or not simdisk's Close returns an error
// (PER-540).
func (f *volFile) Close() error {
	if err := f.enter("Close", simdisk.OpMeta); err != nil {
		return err
	}
	err := f.f.Close()
	f.v.ln.untrack(f)
	return err
}

// enter checks a call of the File method op (PER-510) and samples latency of class class
// (PER-530). Errors name the file's volume path.
func (f *volFile) enter(op string, class simdisk.Op) error {
	if err := f.v.allowed(op, f.f.Name()); err != nil {
		return err
	}
	f.v.sample(class)
	return nil
}

// untrack removes f from ln's open files, if a crash has not cleared them since f was opened.
func (ln *node) untrack(f *volFile) {
	if i := slices.Index(ln.files, f); i >= 0 {
		ln.files = slices.Delete(ln.files, i, i+1)
	}
}

// fileLeaks reports the files the incarnation of attrs, a lockstep.restart or lockstep.stop
// record, still holds open on e's node (SIM-290 c, PER-540): in path order, each is reported with
// a lockstep.leak record and stored in the lockstep/leak slot, and stops being tracked.
func (e *simEngine) fileLeaks(attrs []engine.Attr) {
	ln := e.s.byID[e.n.ID()]
	own := owner{actor: attrValue(attrs, "actor"), inc: attrValue(attrs, "inc")}
	var leaked []*volFile
	kept := ln.files[:0]
	for _, f := range ln.files {
		if f.owner == own {
			leaked = append(leaked, f)
		} else {
			kept = append(kept, f)
		}
	}
	if len(leaked) == 0 {
		return
	}
	clear(ln.files[len(kept):])
	ln.files = kept
	slices.SortStableFunc(leaked, func(a, b *volFile) int { return cmp.Compare(a.f.Name(), b.f.Name()) })
	for _, f := range leaked {
		e.leak(attrs, "file", fmt.Sprintf("file %s on node %s still open", f.f.Name(), e.n.Name()))
	}
}

// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package protoactor

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep/bench/harness"
)

// TestSystemWritesNothing checks that protoactor-go writes nothing to standard output or standard
// error while a system starts, runs a workload and closes. The go command merges a test binary's
// standard error into its standard output, so a line the library wrote during a benchmark would
// land inside a result line and make the budget tool reject it (BEN-030).
func TestSystemWritesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "output")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// protoactor-go's default logger takes os.Stderr when the system is built, so swap both
	// before Start.
	stdout, stderr := os.Stdout, os.Stderr
	os.Stdout, os.Stderr = f, f
	runErr := func() (err error) {
		defer func() { os.Stdout, os.Stderr = stdout, stderr }()
		sys, err := Lib{}.Start()
		if err != nil {
			return err
		}
		defer func() { err = errors.Join(err, sys.Close()) }()
		w, err := sys.PingPong(2)
		if err != nil {
			return err
		}
		if _, err := w.Run(11); err != nil {
			return err
		}
		e, err := sys.Echo(harness.NewRecorder(time.Now(), 1))
		if err != nil {
			return err
		}
		return e.Ask()
	}()
	if runErr != nil {
		t.Fatal(runErr)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) > 0 {
		t.Errorf("the system wrote %q to standard output or standard error", got)
	}
}

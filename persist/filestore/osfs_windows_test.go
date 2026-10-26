// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

//go:build windows

package filestore

import "testing"

func TestATPER42SyncDirWindows(t *testing.T) {
	fsys := &osFS{root: t.TempDir()}
	if err := fsys.SyncDir("."); err != nil {
		t.Fatalf("SyncDir(\".\") = %v, want nil on Windows", err)
	}
}

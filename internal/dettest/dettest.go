// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package dettest is lockstep's determinism lint, import-boundary check, exported-API snapshot
// generator, run-twice switch and golden-hash file format. It is a port of faultline's
// internal/detlint (tag v0.1.2); the tests in this package apply it to the root module.
package dettest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// FindRoot walks up from dir to the first directory whose go.mod declares module path.
func FindRoot(dir, path string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", err
	}
	for d := abs; ; d = filepath.Dir(d) {
		data, err := os.ReadFile(filepath.Join(d, "go.mod"))
		if err == nil && modulePath(string(data)) == path {
			return d, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		if filepath.Dir(d) == d {
			return "", fmt.Errorf("dettest: no go.mod for module %s above %s", path, abs)
		}
	}
}

// modulePath returns the module path declared by go.mod content, or "".
func modulePath(gomod string) string {
	for _, line := range strings.Split(gomod, "\n") {
		line = strings.TrimSpace(line)
		rest, ok := strings.CutPrefix(line, "module")
		if !ok || rest == "" || (rest[0] != ' ' && rest[0] != '\t') {
			continue
		}
		rest, _, _ = strings.Cut(rest, "//")
		rest = strings.TrimSpace(rest)
		if p, err := strconv.Unquote(rest); err == nil {
			return p
		}
		return rest
	}
	return ""
}

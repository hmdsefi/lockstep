// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package golangci_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/golangci/plugin-module-register/register"
	"golang.org/x/tools/go/analysis"

	lsanalysis "github.com/hmdsefi/lockstep/analysis"
	"github.com/hmdsefi/lockstep/analysis/golangci"
)

// build creates the registered plugin with settings and returns its analyzers.
func build(t *testing.T, settings any) []*analysis.Analyzer {
	t.Helper()
	newPlugin, err := register.GetPlugin("lockstep")
	if err != nil {
		t.Fatalf("GetPlugin(lockstep): %v", err)
	}
	p, err := newPlugin(settings)
	if err != nil {
		t.Fatalf("New(%v): %v", settings, err)
	}
	got, err := p.BuildAnalyzers()
	if err != nil {
		t.Fatalf("BuildAnalyzers: %v", err)
	}
	return got
}

func names(as []*analysis.Analyzer) []string {
	out := make([]string, len(as))
	for i, a := range as {
		out[i] = a.Name
	}
	return out
}

func TestPlugin(t *testing.T) {
	t.Run("nil settings", func(t *testing.T) {
		got := build(t, nil)
		if !slices.Equal(got, lsanalysis.All()) {
			t.Errorf("BuildAnalyzers = %v, want All() = %v", names(got), names(lsanalysis.All()))
		}
		newPlugin, err := register.GetPlugin("lockstep")
		if err != nil {
			t.Fatal(err)
		}
		p, err := newPlugin(nil)
		if err != nil {
			t.Fatal(err)
		}
		if mode := p.GetLoadMode(); mode != "typesinfo" {
			t.Errorf("GetLoadMode = %q, want %q", mode, "typesinfo")
		}
	})

	t.Run("disable", func(t *testing.T) {
		got := build(t, map[string]any{"disable": []any{"determinism"}})
		want := []*analysis.Analyzer{lsanalysis.Directives, lsanalysis.AwaitInActor}
		if !slices.Equal(got, want) {
			t.Errorf("BuildAnalyzers = %v, want %v", names(got), names(want))
		}
	})

	t.Run("typed settings", func(t *testing.T) {
		got := build(t, golangci.Settings{Disable: []string{"directives", "awaitinactor"}})
		want := []*analysis.Analyzer{lsanalysis.Determinism}
		if !slices.Equal(got, want) {
			t.Errorf("BuildAnalyzers = %v, want %v", names(got), names(want))
		}
	})

	t.Run("unknown analyzer", func(t *testing.T) {
		_, err := golangci.New(map[string]any{"disable": []any{"nope"}})
		const want = `lockstep: unknown analyzer "nope" in settings.disable; known: directives, determinism, awaitinactor`
		if err == nil || err.Error() != want {
			t.Errorf("New error = %v, want %q", err, want)
		}
	})

	t.Run("unknown field", func(t *testing.T) {
		_, err := golangci.New(map[string]any{"bogus": 1})
		if err == nil || !strings.HasPrefix(err.Error(), "lockstep: settings: ") {
			t.Errorf("New error = %v, want prefix %q", err, "lockstep: settings: ")
		}
	})
}

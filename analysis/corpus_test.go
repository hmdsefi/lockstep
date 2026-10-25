// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/analysistest"
)

// runCorpus runs analyzer a on the corpus packages pkgs of testdata/src and checks the diagnostics
// against their want comments. It also checks the diagnostic format (ANL-110, AT-ANL-08): every
// diagnostic's Category equals the text of its Message before the first colon.
func runCorpus(t *testing.T, a *analysis.Analyzer, pkgs ...string) {
	t.Helper()
	for _, res := range analysistest.Run(t, analysistest.TestData(), a, pkgs...) {
		for _, d := range res.Diagnostics {
			if prefix, _, _ := strings.Cut(d.Message, ":"); d.Category != prefix {
				t.Errorf("%s: category %q, want %q (message %q)",
					res.Pass.Fset.Position(d.Pos), d.Category, prefix, d.Message)
			}
		}
	}
}

// corpusIDs are the check IDs of this release that have a bad and a good corpus package (ANL-380).
var corpusIDs = []string{"ls001", "ls002", "ls100", "ls110"}

// TestCorpusComplete checks AT-ANL-26: for every check ID of the release, testdata/src holds the
// packages lsNNN/bad and lsNNN/good with at least one .go file each; every bad package has a want
// comment and no good package has one.
func TestCorpusComplete(t *testing.T) {
	src := filepath.Join(analysistest.TestData(), "src")
	for _, id := range corpusIDs {
		for _, kind := range []string{"bad", "good"} {
			pkg := id + "/" + kind
			files, err := filepath.Glob(filepath.Join(src, id, kind, "*.go"))
			if err != nil {
				t.Fatal(err)
			}
			if len(files) == 0 {
				t.Errorf("testdata/src/%s: no .go file", pkg)
				continue
			}
			wants := 0
			for _, f := range files {
				wants += countWants(t, f)
			}
			if kind == "bad" && wants == 0 {
				t.Errorf("testdata/src/%s: no want comment", pkg)
			}
			if kind == "good" && wants != 0 {
				t.Errorf("testdata/src/%s: %d want comments, want none", pkg, wants)
			}
		}
	}
}

// countWants returns the number of want comments in the Go file at path. A comment is a want
// comment when its text, after the // or /* marker and any spaces, starts with "want", or when it
// contains "// want": the rule analysistest uses to find expectations.
func countWants(t *testing.T, path string) int {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, group := range f.Comments {
		for _, c := range group.List {
			if isWantComment(c.Text) {
				n++
			}
		}
	}
	return n
}

// isWantComment reports whether the comment text (with its // or /* */ markers) is a want comment.
func isWantComment(text string) bool {
	body, ok := strings.CutPrefix(text, "//")
	if !ok {
		body = strings.TrimSuffix(strings.TrimPrefix(text, "/*"), "*/")
	}
	if strings.Contains(body, "// want") {
		return true
	}
	return strings.HasPrefix(strings.TrimSpace(body), "want")
}

// TestIsWantComment checks the want-comment rule of TestCorpusComplete on both comment forms.
func TestIsWantComment(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{"// want `x`", true},
		{"//want `x`", true},
		{"/* want `x` */", true},
		{"/*want `x`*/", true},
		{"// note // want `x`", true},
		{"// a want", false},
		{"/* note */", false},
		{"//lockstep:allow LS100 want", false},
	}
	for _, tt := range tests {
		if got := isWantComment(tt.text); got != tt.want {
			t.Errorf("isWantComment(%q) = %v, want %v", tt.text, got, tt.want)
		}
	}
}

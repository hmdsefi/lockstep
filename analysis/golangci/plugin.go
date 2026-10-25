// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Package golangci registers lockstep's analyzers as the golangci-lint module plugin "lockstep".
package golangci

import (
	"fmt"
	"slices"
	"strings"

	"github.com/golangci/plugin-module-register/register"
	"golang.org/x/tools/go/analysis"

	lsanalysis "github.com/hmdsefi/lockstep/analysis"
)

func init() { register.Plugin("lockstep", New) } //nolint:gochecknoinits // golangci-lint module plugins register in init

// Settings is the plugin configuration under linters.settings.custom.lockstep.settings.
type Settings struct {
	// Disable lists analyzer names (for example "exhaustive") that the plugin leaves out.
	Disable []string `json:"disable"`
}

// New decodes settings with register.DecodeSettings[Settings] and returns the plugin. It returns
// an error for unknown fields and for unknown analyzer names in Disable (ANL-410).
func New(settings any) (register.LinterPlugin, error) {
	s, err := register.DecodeSettings[Settings](settings)
	if err != nil {
		return nil, fmt.Errorf("lockstep: settings: %w", err)
	}
	all := lsanalysis.All()
	known := make([]string, len(all))
	for i, a := range all {
		known[i] = a.Name
	}
	for _, name := range s.Disable {
		if !slices.Contains(known, name) {
			return nil, fmt.Errorf("lockstep: unknown analyzer %q in settings.disable; known: %s",
				name, strings.Join(known, ", "))
		}
	}
	enabled := make([]*analysis.Analyzer, 0, len(all))
	for _, a := range all {
		if !slices.Contains(s.Disable, a.Name) {
			enabled = append(enabled, a)
		}
	}
	return &plugin{analyzers: enabled}, nil
}

type plugin struct{ analyzers []*analysis.Analyzer }

// BuildAnalyzers returns analysis.All() without the disabled analyzers, in All's order.
func (p *plugin) BuildAnalyzers() ([]*analysis.Analyzer, error) {
	return slices.Clone(p.analyzers), nil
}

// GetLoadMode returns register.LoadModeTypesInfo.
func (p *plugin) GetLoadMode() string { return register.LoadModeTypesInfo }

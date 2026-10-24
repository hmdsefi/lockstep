// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package analysis

import (
	"fmt"
	"slices"
	"strings"

	"golang.org/x/mod/modfile"
)

// faultlineVersion is the faultline release whose determinism rules faultlineRules maps to the
// LS100 sub-rules (ANL-150). The root module requires the same version.
const faultlineVersion = "v0.1.2"

// rootModulePath and faultlineModulePath name the modules whose go.mod requirement is compared
// with faultlineVersion (ANL-160).
const (
	rootModulePath      = "github.com/hmdsefi/lockstep"
	faultlineModulePath = "github.com/hmdsefi/faultline"
)

// The dispositions of a faultline rule in LS100 (ANL-150).
const (
	// dispositionAdopted ports the rule unchanged.
	dispositionAdopted = "adopted"
	// dispositionAdapted ports the rule with a change for actor code.
	dispositionAdapted = "adapted"
	// dispositionExcluded leaves the rule out and says why.
	dispositionExcluded = "excluded"
)

// snapshotHeaderTail is the end of the first line of the snapshot, after "faultline <version> ".
const snapshotHeaderTail = `AGENTS.md "Determinism rules"`

// faultlineRule maps one bullet of the "Determinism rules" section of faultline's agent guide
// (AGENTS.md) to LS100 (ANL-150).
type faultlineRule struct {
	// text is the bullet without its "- " marker, continuation lines joined, whitespace
	// collapsed to single spaces.
	text string
	// lintIDs are the faultline lint IDs that enforce the bullet, for reference only.
	lintIDs []string
	// disposition is dispositionAdopted, dispositionAdapted or dispositionExcluded.
	disposition string
	// subRules are the LS100 sub-rules of an adopted or adapted rule.
	subRules []string
	// reason says why an excluded rule is left out.
	reason string
}

// faultlineRules holds one entry per bullet of faultline's determinism rules at faultlineVersion,
// in the order of the snapshot.
var faultlineRules = []faultlineRule{
	{
		text: "never uses `time.Now`, `time.Since`, `time.Until`, `time.Sleep`, `time.After`, " +
			"`time.AfterFunc`, `time.Tick`, timers or tickers: virtual time comes from `Sim.Now`, " +
			"`Node.Now` and `Node.After`;",
		lintIDs:     []string{"DL001"},
		disposition: dispositionAdopted,
		subRules:    []string{"wallclock"},
	},
	{
		text: "never uses the package-level functions of `math/rand` or `math/rand/v2`, or `crypto/rand`: " +
			"all randomness comes from `Sim.Rand(label)` and `Node.Rand()` streams (labels in " +
			"architecture §7; the prefixes `kernel/` and `node/` are reserved for the kernel);",
		lintIDs:     []string{"DL002", "DL003"},
		disposition: dispositionAdapted,
		// Actor code may import crypto/rand; LS100 reports its uses, not the import.
		subRules: []string{"globalrand", "cryptorand"},
	},
	{
		text: "never starts a goroutine, never uses `select`, and never calls `runtime.SetFinalizer` or " +
			"`runtime.AddCleanup` (they run a function on a runtime goroutine);",
		lintIDs:     []string{"DL004", "DL005"},
		disposition: dispositionAdopted,
		subRules:    []string{"goroutine", "select"},
	},
	{
		text: "never ranges over a map, and never uses `maps.Keys`, `maps.Values`, `maps.All`, " +
			"`reflect.Value.MapKeys`, `reflect.Value.MapRange`, `reflect.Value.Seq`, `reflect.Value.Seq2` or " +
			"`sync.Map.Range`, without sorting; never calls `maps.DeleteFunc` or `maps.EqualFunc`, which call " +
			"a function in map order; a justified exception carries a `//faultline:maporder <reason>` comment;",
		lintIDs:     []string{"DL006", "DL009", "DL010"},
		disposition: dispositionAdapted,
		// The exception is `//lockstep:allow LS100 <reason>`, not `//faultline:maporder`.
		subRules: []string{"maprange", "unordered", "mapiter"},
	},
	{
		text: "never uses floating point in decisions: probabilities are `uint32` parts per million " +
			"(`kernel.Chance`), durations are integer nanoseconds (`kernel.Uniform`);",
		lintIDs:     []string{"DL007"},
		disposition: dispositionExcluded,
		reason:      "actor business logic may need floating point and lockstep offers no integer replacement",
	},
	{
		text: "never reads environment variables or the user's folders (`os.TempDir`, `os.UserHomeDir`, " +
			"`os.UserCacheDir`, `os.UserConfigDir`); only package `faultline` may;",
		lintIDs:     []string{"DL008"},
		disposition: dispositionAdapted,
		// No package is exempt.
		subRules: []string{"env"},
	},
	{
		text: "emits a `kernel.Record` for every simulated decision and describes payloads with " +
			"`kernel.Describe`, never with `%v` of arbitrary values (pointer addresses differ between runs);",
		disposition: dispositionExcluded,
		reason:      "applies to code that emits records; DET enforces it for lockstep's own packages",
	},
	{
		text:        "has no GOOS- or GOARCH-specific files.",
		lintIDs:     []string{"DL011"},
		disposition: dispositionExcluded,
		reason:      "a file-level rule for faultline's packages; users choose their build constraints",
	},
}

// compareRules returns the first ANL-160 error found when the snapshot of faultline's rules
// (the content of testdata/faultline/determinism-rules.md) and the root module's go.mod are
// compared with faultlineVersion, faultlineRules and ls100SubRules, or nil. rootGoMod is nil
// when the root module is not around.
func compareRules(snapshot, rootGoMod []byte) error {
	return compareRulesWith(snapshot, rootGoMod, faultlineVersion, faultlineRules, ls100SubRules)
}

// compareRulesWith is compareRules with the pinned version, the rule entries and the sub-rule
// table as parameters, so tests can change them.
func compareRulesWith(snapshot, rootGoMod []byte, version string, rules []faultlineRule, subRules []subRule) error {
	snapshotVersion, bullets, err := parseRulesSnapshot(snapshot)
	if err != nil {
		return err
	}

	// 1. The snapshot is for the pinned version.
	if snapshotVersion != version {
		return fmt.Errorf("analysis: snapshot is faultline %s, rules.go pins %s", snapshotVersion, version)
	}

	// 2. The root module requires the pinned version. Skipped outside the repository.
	required, err := rootFaultlineVersion(rootGoMod)
	if err != nil {
		return err
	}
	if required != "" && required != version {
		return fmt.Errorf("analysis: root module requires faultline %s, rules.go pins %s; "+
			"copy the \"Determinism rules\" section of faultline %s into "+
			"analysis/testdata/faultline/determinism-rules.md and review LS100",
			required, version, required)
	}

	// 3. The snapshot and rules.go have the same number of rules.
	if len(bullets) != len(rules) {
		return fmt.Errorf("analysis: snapshot has %d faultline rules, rules.go has %d", len(bullets), len(rules))
	}

	// 4. Every bullet is the text rules.go maps.
	for i, bullet := range bullets {
		if bullet != rules[i].text {
			return fmt.Errorf("analysis: faultline rule %d changed:\n  snapshot: %s\n  rules.go: %s",
				i+1, bullet, rules[i].text)
		}
	}

	// 5. An adopted or adapted rule names a sub-rule.
	for i, rule := range rules {
		if rule.disposition != dispositionExcluded && len(rule.subRules) == 0 {
			return fmt.Errorf("analysis: faultline rule %d is %s but names no sub-rule", i+1, rule.disposition)
		}
	}

	// 6. An excluded rule says why.
	for i, rule := range rules {
		if rule.disposition == dispositionExcluded && rule.reason == "" {
			return fmt.Errorf("analysis: faultline rule %d is excluded without a reason", i+1)
		}
	}

	// 7. Every sub-rule that ports a faultline rule appears in exactly one entry.
	for _, sub := range subRules {
		if sub.origin == originLockstep {
			continue
		}
		mapped := 0
		for _, rule := range rules {
			if slices.Contains(rule.subRules, sub.name) {
				mapped++
			}
		}
		switch {
		case mapped == 0:
			return fmt.Errorf("analysis: LS100 sub-rule %s is not mapped to a faultline rule", sub.name)
		case mapped > 1:
			return fmt.Errorf("analysis: LS100 sub-rule %s is mapped to more than one faultline rule", sub.name)
		}
	}
	return nil
}

// parseRulesSnapshot reads the faultline version from the header line of the snapshot and the
// bullets of its body (ANL-150): a bullet is a line starting with "- " plus the following lines
// starting with two spaces, with whitespace collapsed to single spaces and the "- " dropped.
func parseRulesSnapshot(snapshot []byte) (version string, bullets []string, err error) {
	lines := strings.Split(strings.ReplaceAll(string(snapshot), "\r\n", "\n"), "\n")
	header := lines[0]
	rest, ok := strings.CutPrefix(header, "faultline ")
	version, tail, found := strings.Cut(rest, " ")
	if !ok || !found || version == "" || tail != snapshotHeaderTail {
		return "", nil, fmt.Errorf("analysis: snapshot header %q is not 'faultline <version> %s'", header, snapshotHeaderTail)
	}
	return version, bulletsOf(lines[1:]), nil
}

// bulletsOf splits the body of the snapshot into bullets.
func bulletsOf(lines []string) []string {
	var bullets []string
	open := false // the previous line belongs to a bullet
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "- "):
			bullets = append(bullets, strings.Join(strings.Fields(line[2:]), " "))
			open = true
		case open && strings.HasPrefix(line, "  "):
			last := len(bullets) - 1
			bullets[last] = strings.Join(strings.Fields(bullets[last]+" "+line), " ")
		default:
			open = false
		}
	}
	return bullets
}

// rootFaultlineVersion returns the version of faultline the root go.mod requires. It returns ""
// when rootGoMod is nil, names another module (the analysis module is outside the repository, for
// example in the module cache) or has no faultline requirement.
func rootFaultlineVersion(rootGoMod []byte) (string, error) {
	if rootGoMod == nil {
		return "", nil
	}
	file, err := modfile.Parse("go.mod", rootGoMod, nil)
	if err != nil {
		return "", fmt.Errorf("analysis: reading the root go.mod: %w", err)
	}
	if file.Module == nil || file.Module.Mod.Path != rootModulePath {
		return "", nil
	}
	for _, req := range file.Require {
		if req.Mod.Path == faultlineModulePath {
			return req.Mod.Version, nil
		}
	}
	return "", nil
}

// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package sim_test

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/lockstep/sim"
)

// envModuleTest is the test file of the user module that AT-SIM-03 (a) builds. It has no
// TestMain, so only sim reads LOCKSTEP_CHECK_DETERMINISM. Each test prints how often its body ran.
const envModuleTest = `package simenv_test

import (
	"fmt"
	"testing"

	"github.com/hmdsefi/faultline"
	"github.com/hmdsefi/lockstep/sim"
)

func count(t *testing.T) func(*sim.Sim) {
	calls := 0
	t.Cleanup(func() { fmt.Printf("%s body calls: %d\n", t.Name(), calls) })
	return func(*sim.Sim) { calls++ }
}

func TestSimulate(t *testing.T) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Seeds: 3}}, count(t))
}

func TestSimulateSeeds(t *testing.T) {
	sim.SimulateSeeds(t, sim.Options{}, count(t), 1, 2)
}

func TestInvalidMode(t *testing.T) {
	sim.Simulate(t, sim.Options{Options: faultline.Options{Mode: faultline.ModeGoroutine}}, count(t))
}
`

// buildEnvModule writes a module that requires lockstep through a replace of the repository
// root into a temporary directory, builds its test binary offline and returns the binary's path.
func buildEnvModule(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	gomod, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	gosum, err := os.ReadFile(filepath.Join(root, "go.sum"))
	if err != nil {
		t.Fatal(err)
	}
	// The root go.mod is copied verbatim apart from its module line: its requirements pin
	// faultline and the rest of the build list, so nothing is downloaded. A relative replace or
	// tool directive in it would not resolve from the temporary directory.
	const self = "module github.com/hmdsefi/lockstep\n"
	if !strings.HasPrefix(string(gomod), self) {
		t.Fatalf("%s/go.mod does not start with %q", root, self)
	}
	mod := "module example.com/simenv\n" + strings.TrimPrefix(string(gomod), self) +
		"\nrequire github.com/hmdsefi/lockstep v0.0.0\n\nreplace github.com/hmdsefi/lockstep => " + strconv.Quote(root) + "\n"

	dir := t.TempDir()
	for _, f := range []struct{ name, data string }{
		{"go.mod", mod}, {"go.sum", string(gosum)}, {"simenv_test.go", envModuleTest},
	} {
		if err := os.WriteFile(filepath.Join(dir, f.name), []byte(f.data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// go test puts its own GOROOT/bin first on PATH, so this is the go that built this binary;
	// with GOTOOLCHAIN=local it must be at least go.mod's go version.
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(dir, "simenv.test")
	cmd := exec.Command(goBin, "test", "-c", "-o", bin, ".") //nolint:gosec // the go command on PATH
	cmd.Dir = dir
	cmd.Env = append(childEnv("GOROOT", "GOFLAGS", "GOWORK", "GOPROXY", "GOTOOLCHAIN"),
		"GOFLAGS=-mod=mod", "GOWORK=off", "GOPROXY=off", "GOTOOLCHAIN=local")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go test -c in %s: %v\n%s", dir, err, out)
	}
	return bin
}

// childEnv returns the environment of this process without the named variables and without any
// FAULTLINE_* or LOCKSTEP_* variable.
func childEnv(drop ...string) []string {
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		if strings.HasPrefix(k, "FAULTLINE_") || strings.HasPrefix(k, "LOCKSTEP_") || slices.Contains(drop, k) {
			continue
		}
		env = append(env, kv)
	}
	return env
}

// bodyCalls matches the lines the tests of envModuleTest print.
var bodyCalls = regexp.MustCompile(`(?m)^(Test\w+) body calls: (\d+)$`)

// AT-SIM-03, SIM-030, DET-145.
func TestATSIM03EnvironmentSwitch(t *testing.T) {
	t.Run("user module", func(t *testing.T) {
		if testing.Short() {
			t.Skip("builds a module with the go command; skipped under -short")
		}
		const invalid = `lockstep: sim: invalid LOCKSTEP_CHECK_DETERMINISM="yes": want 0 or 1`
		const mode = "lockstep: sim: Options.Mode must be faultline.ModeEvent"
		bin := buildEnvModule(t)
		for _, tc := range []struct {
			name   string
			env    []string
			run    string
			fail   bool
			want   map[string]int // body calls by test
			output string         // a line the output must contain
			absent string         // text the output must not contain
		}{
			{name: "on", env: []string{"LOCKSTEP_CHECK_DETERMINISM=1"}, run: "^TestSimulate(Seeds)?$",
				want: map[string]int{"TestSimulate": 6, "TestSimulateSeeds": 4}},
			{name: "off", env: []string{"LOCKSTEP_CHECK_DETERMINISM=0"}, run: "^TestSimulate(Seeds)?$",
				want: map[string]int{"TestSimulate": 3, "TestSimulateSeeds": 2}},
			{name: "unset", run: "^TestSimulate(Seeds)?$",
				want: map[string]int{"TestSimulate": 3, "TestSimulateSeeds": 2}},
			{name: "invalid", env: []string{"LOCKSTEP_CHECK_DETERMINISM=yes"}, run: "^TestSimulate(Seeds)?$", fail: true,
				want: map[string]int{"TestSimulate": 0, "TestSimulateSeeds": 0}, output: invalid},
			{name: "options checked first", env: []string{"LOCKSTEP_CHECK_DETERMINISM=yes"}, run: "^TestInvalidMode$", fail: true,
				want: map[string]int{"TestInvalidMode": 0}, output: mode, absent: "LOCKSTEP_CHECK_DETERMINISM"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cmd := exec.Command(bin, "-test.run="+tc.run, "-test.v") //nolint:gosec // the binary this test built
				cmd.Env = append(childEnv(), "FAULTLINE_ARTIFACTS="+t.TempDir())
				cmd.Env = append(cmd.Env, tc.env...)
				raw, err := cmd.CombinedOutput()
				out := string(raw)
				var exit *exec.ExitError
				if err != nil && !errors.As(err, &exit) {
					t.Fatal(err)
				}
				if failed := err != nil; failed != tc.fail {
					t.Fatalf("child failed = %v, want %v\n%s", failed, tc.fail, out)
				}
				got := map[string]int{}
				for _, m := range bodyCalls.FindAllStringSubmatch(out, -1) {
					n, _ := strconv.Atoi(m[2])
					got[m[1]] = n
				}
				if len(got) != len(tc.want) {
					t.Errorf("body calls %v, want %v\n%s", got, tc.want, out)
				}
				for _, name := range slices.Sorted(maps.Keys(tc.want)) {
					n := tc.want[name]
					if g, ok := got[name]; !ok || g != n {
						t.Errorf("%s: body calls %d (reported %v), want %d\n%s", name, g, ok, n, out)
					}
				}
				if tc.output != "" && !strings.Contains(out, tc.output) {
					t.Errorf("output lacks %q:\n%s", tc.output, out)
				}
				if tc.absent != "" && strings.Contains(out, tc.absent) {
					t.Errorf("output contains %q:\n%s", tc.absent, out)
				}
			})
		}
	})

	t.Run("reads only in env.go", func(t *testing.T) {
		for _, p := range envReads(t, ".") {
			t.Error(p)
		}
	})
}

// envReads returns one problem per call to os.Getenv, os.LookupEnv, os.Environ or syscall.Getenv
// in the non-test files of the package in dir that SIM-030 does not allow: a call outside env.go,
// or a call in env.go whose argument is not the literal name LOCKSTEP_CHECK_DETERMINISM or
// LOCKSTEP_WORKLOAD, or a second read of one name. It also reports env.go not reading
// LOCKSTEP_CHECK_DETERMINISM. It finds calls through the package name only: dot-imports and
// function values (f := os.Getenv) are not detected, as SIM-030 is about calls.
func envReads(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	funcs := map[string][]string{"os": {"Getenv", "LookupEnv", "Environ"}, "syscall": {"Getenv"}}
	allowed := map[string]bool{"LOCKSTEP_CHECK_DETERMINISM": true, "LOCKSTEP_WORKLOAD": true}
	read := map[string]int{}
	var problems []string
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		local := map[string]string{} // local import name -> "os" or "syscall"
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			if _, ok := funcs[path]; !ok {
				continue
			}
			n := path
			if imp.Name != nil {
				n = imp.Name.Name
			}
			local[n] = path
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			x, ok := sel.X.(*ast.Ident)
			if !ok || local[x.Name] == "" || !slices.Contains(funcs[local[x.Name]], sel.Sel.Name) {
				return true
			}
			at := fset.Position(call.Pos()).String()
			fn := local[x.Name] + "." + sel.Sel.Name
			if name != "env.go" {
				problems = append(problems, at+": "+fn+" outside env.go")
				return true
			}
			var v string
			if len(call.Args) == 1 {
				if lit, ok := call.Args[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
					v, _ = strconv.Unquote(lit.Value)
				}
			}
			switch {
			case !allowed[v]:
				problems = append(problems, at+": "+fn+" must read LOCKSTEP_CHECK_DETERMINISM or LOCKSTEP_WORKLOAD by literal name")
			case read[v] > 0:
				problems = append(problems, at+": second read of "+v)
			}
			read[v]++
			return true
		})
	}
	if read["LOCKSTEP_CHECK_DETERMINISM"] == 0 {
		problems = append(problems, "env.go does not read LOCKSTEP_CHECK_DETERMINISM")
	}
	return problems
}

// SIM-030: the values of LOCKSTEP_CHECK_DETERMINISM, in this process.
func TestSIM030ApplyEnv(t *testing.T) {
	for _, tc := range []struct {
		value   string
		want    bool
		wantErr string
	}{
		{value: "", want: false},
		{value: "0", want: false},
		{value: "1", want: true},
		{value: "yes", wantErr: `lockstep: sim: invalid LOCKSTEP_CHECK_DETERMINISM="yes": want 0 or 1`},
		{value: "01", wantErr: `lockstep: sim: invalid LOCKSTEP_CHECK_DETERMINISM="01": want 0 or 1`},
	} {
		t.Run(strconv.Quote(tc.value), func(t *testing.T) {
			t.Setenv("LOCKSTEP_CHECK_DETERMINISM", tc.value)
			o, err := sim.ApplyEnv(faultline.Options{Seeds: 7})
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("error %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if o.CheckDeterminism != tc.want || o.Seeds != 7 {
				t.Errorf("options %+v, want CheckDeterminism %v and Seeds 7", o, tc.want)
			}
		})
	}
}

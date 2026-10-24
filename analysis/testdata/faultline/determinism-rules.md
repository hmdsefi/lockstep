faultline v0.1.2 AGENTS.md "Determinism rules"

The same seed, code and Go minor version give the same trace hash on linux/amd64 and darwin/arm64.
Simulation code (`kernel`, `kernel/*`, `check/*`, `internal/toys`, and the other core and tool
packages listed in `internal/detlint`):

- never uses `time.Now`, `time.Since`, `time.Until`, `time.Sleep`, `time.After`, `time.AfterFunc`,
  `time.Tick`, timers or tickers: virtual time comes from `Sim.Now`, `Node.Now` and `Node.After`;
- never uses the package-level functions of `math/rand` or `math/rand/v2`, or `crypto/rand`: all
  randomness comes from `Sim.Rand(label)` and `Node.Rand()` streams (labels in architecture §7; the
  prefixes `kernel/` and `node/` are reserved for the kernel);
- never starts a goroutine, never uses `select`, and never calls `runtime.SetFinalizer` or
  `runtime.AddCleanup` (they run a function on a runtime goroutine);
- never ranges over a map, and never uses `maps.Keys`, `maps.Values`, `maps.All`,
  `reflect.Value.MapKeys`, `reflect.Value.MapRange`, `reflect.Value.Seq`, `reflect.Value.Seq2` or
  `sync.Map.Range`, without sorting; never calls `maps.DeleteFunc` or `maps.EqualFunc`, which call
  a function in map order; a justified exception carries a `//faultline:maporder <reason>` comment;
- never uses floating point in decisions: probabilities are `uint32` parts per million
  (`kernel.Chance`), durations are integer nanoseconds (`kernel.Uniform`);
- never reads environment variables or the user's folders (`os.TempDir`, `os.UserHomeDir`,
  `os.UserCacheDir`, `os.UserConfigDir`); only package `faultline` may;
- emits a `kernel.Record` for every simulated decision and describes payloads with
  `kernel.Describe`, never with `%v` of arbitrary values (pointer addresses differ between runs);
- has no GOOS- or GOARCH-specific files.

`internal/detlint` enforces these rules (`TestDeterminismLint`) and the import rules of
architecture §5.1 (`TestImportBoundaries`). A new package directory needs a class and an import
rule in `internal/detlint/module.go` and an entry in `internal/detlint/inventory_test.go`.

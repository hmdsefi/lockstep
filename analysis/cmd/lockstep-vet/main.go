// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

// Command lockstep-vet runs lockstep's analyzers. Use it with go vet:
//
//	go vet -vettool=$(go env GOPATH)/bin/lockstep-vet ./...
//
// or on its own: lockstep-vet ./...
package main

import (
	lsanalysis "github.com/hmdsefi/lockstep/analysis"
	"golang.org/x/tools/go/analysis/multichecker"
)

func main() { multichecker.Main(lsanalysis.All()...) }

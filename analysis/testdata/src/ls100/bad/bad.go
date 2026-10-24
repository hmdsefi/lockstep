// Package bad holds the LS100 findings of the wall clock, randomness, goroutine, select and
// environment sub-rules (ANL-130, AT-ANL-09), each line with the diagnostic it gets.
package bad

import (
	"context"
	crand "crypto/rand"
	mrand "math/rand"
	"math/rand/v2"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/hmdsefi/lockstep"
)

// Msg is Account's message type.
type Msg int

// Account is an actor type with all four roots.
type Account struct{}

// Started is declared first, so the search reaches stamp from Started; Receive calls stamp too,
// and stamp's finding is reported once (AT-ANL-08).
func (a *Account) Started(ctx *lockstep.Context[Msg]) {
	stamp()
	_ = os.Getenv("HOME") // want `^LS100: os\.Getenv reads the environment; read configuration before spawning and pass it through the factory \(actor code: \(\*Account\)\.Started\)$`
}

// Receive holds one finding per row. The literals passed to wg.Go, context.AfterFunc and
// runtime.SetFinalizer are actor code too; they come after the first literal and stay empty.
func (a *Account) Receive(ctx *lockstep.Context[Msg], msg Msg) {
	record := func() {
		_ = time.Now() // want `^LS100: time\.Now uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive -> \(\*Account\)\.Receive\$1\)$`
	}
	record()
	_ = time.Now() // want `^LS100: time\.Now uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive\)$`
	f := time.Now  // want `^LS100: time\.Now uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive\)$`
	_ = f
	_ = rand.IntN(3) // want `^LS100: math/rand/v2\.IntN uses global randomness; use ctx\.Rand\(\) \(actor code: \(\*Account\)\.Receive\)$`
	buf := make([]byte, 8)
	_, _ = crand.Read(buf) // want `^LS100: crypto/rand\.Read is nondeterministic; use ctx\.Rand\(\) \(actor code: \(\*Account\)\.Receive\)$`
	go a.loop()            // want `^LS100: go statement starts a goroutine; use lockstep\.Do for work outside the turn \(actor code: \(\*Account\)\.Receive\)$`
	var wg sync.WaitGroup
	wg.Go(func() {})                                       // want `^LS100: \(\*sync\.WaitGroup\)\.Go starts a goroutine; use lockstep\.Do for work outside the turn \(actor code: \(\*Account\)\.Receive\)$`
	_ = context.AfterFunc(context.Background(), func() {}) // want `^LS100: context\.AfterFunc starts a goroutine; use lockstep\.Do for work outside the turn \(actor code: \(\*Account\)\.Receive\)$`
	runtime.SetFinalizer(a, func(*Account) {})             // want `^LS100: runtime\.SetFinalizer runs a function on a runtime goroutine; release resources in Stopped \(actor code: \(\*Account\)\.Receive\)$`
	go func() { _ = time.Now() }()                         // want `^LS100: go statement starts a goroutine; use lockstep\.Do for work outside the turn \(actor code: \(\*Account\)\.Receive\)$`
	stamp()
	more()
}

// Stopped uses the crypto/rand variable Reader.
func (a *Account) Stopped(ctx *lockstep.Context[Msg], r lockstep.StopReason) {
	_ = crand.Reader // want `^LS100: crypto/rand\.Reader is nondeterministic; use ctx\.Rand\(\) \(actor code: \(\*Account\)\.Stopped\)$`
}

// Signal ends in a select statement.
func (a *Account) Signal(ctx *lockstep.Context[Msg], s lockstep.Signal) {
	select {} // want `^LS100: select statement waits on channels; actors get input only as messages \(actor code: \(\*Account\)\.Signal\)$`
}

// loop is the operand of a go statement, so it is not actor code.
func (a *Account) loop() {
	_ = time.Now()
}

// stamp is reached from Started and from Receive.
func stamp() {
	_ = time.Since(time.Time{}) // want `^LS100: time\.Since uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Started -> stamp\)$`
}

// more holds further names of the same sub-rules.
func more() {
	c, cancel := context.WithTimeout(context.Background(), time.Second) // want `^LS100: context\.WithTimeout uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive -> more\)$`
	defer cancel()
	_ = c
	time.Sleep(time.Millisecond)                  // want `^LS100: time\.Sleep uses the wall clock; use ctx\.Now, ctx\.After or ctx\.Every \(actor code: \(\*Account\)\.Receive -> more\)$`
	_ = mrand.Intn(3)                             // want `^LS100: math/rand\.Intn uses global randomness; use ctx\.Rand\(\) \(actor code: \(\*Account\)\.Receive -> more\)$`
	_ = rand.N(time.Second)                       // want `^LS100: math/rand/v2\.N uses global randomness; use ctx\.Rand\(\) \(actor code: \(\*Account\)\.Receive -> more\)$`
	_ = crand.Text()                              // want `^LS100: crypto/rand\.Text is nondeterministic; use ctx\.Rand\(\) \(actor code: \(\*Account\)\.Receive -> more\)$`
	runtime.AddCleanup(new(int), func(int) {}, 0) // want `^LS100: runtime\.AddCleanup runs a function on a runtime goroutine; release resources in Stopped \(actor code: \(\*Account\)\.Receive -> more\)$`
	_, _ = os.LookupEnv("HOME")                   // want `^LS100: os\.LookupEnv reads the environment; read configuration before spawning and pass it through the factory \(actor code: \(\*Account\)\.Receive -> more\)$`
	_ = os.TempDir()                              // want `^LS100: os\.TempDir reads the environment; read configuration before spawning and pass it through the factory \(actor code: \(\*Account\)\.Receive -> more\)$`
	_, _ = syscall.Getenv("HOME")                 // want `^LS100: syscall\.Getenv reads the environment; read configuration before spawning and pass it through the factory \(actor code: \(\*Account\)\.Receive -> more\)$`
}

// Package refs declares types whose values do or do not share memory when copied. A probe reports
// "refs=<bool>" at every package-level type spec: the result of containsRefs for its type. The
// aliases name the types they stand for, so the probe sees string, time.Time and so on exactly.
package refs

import (
	"facts/marked"
	"time"
	"unsafe"

	"github.com/hmdsefi/lockstep"
)

// Safe types: copying them shares nothing the receiver can write.

type str = string                    // want `^refs=false$`
type clock = time.Time               // want `^refs=false$`
type pid = lockstep.PID[int]         // want `^refs=false$`
type address = lockstep.Address      // want `^refs=false$`
type replyTo = lockstep.ReplyTo[int] // want `^refs=false$`

// Batch has reference fields, but its marker makes it safe.
//
//lockstep:immutable
type Batch struct { // want `^refs=false$`
	Items []int
	Tags  map[string]string
}

type batchPtr = *Batch // want `^refs=false$`

// Frozen is a generic marked type; its instances are marked too.
//
//lockstep:immutable
type Frozen[T any] struct { // want `^refs=false$`
	Values []T
}

type frozenInts = Frozen[int] // want `^refs=false$`

// Markers of imported types come from their facts.
type importedBatch = marked.Batch           // want `^refs=false$`
type importedBatchPtr = *marked.Batch       // want `^refs=false$`
type importedFrozen = marked.Frozen[string] // want `^refs=false$`
type importedPlain = marked.Plain           // want `^refs=true$`
type importedWrapper = marked.Wrapper       // want `^refs=false$`
type importedPlainPtr = *marked.Plain       // want `^refs=true$`

// A resource marker does not make a type safe.
//
//lockstep:resource
type Handle struct { // want `^refs=true$`
	buf []byte
}

// Reference types.

type ptr = *int                  // want `^refs=true$`
type slice = []int               // want `^refs=true$`
type mapping = map[string]int    // want `^refs=true$`
type channel = chan int          // want `^refs=true$`
type function = func()           // want `^refs=true$`
type rawPointer = unsafe.Pointer // want `^refs=true$`
type pointerArray = [4]*int      // want `^refs=true$`

type withSlice struct { // want `^refs=true$`
	n int
	s []string
}

type pointerToUnmarked = *withSlice // want `^refs=true$`

type nested struct { // want `^refs=true$`
	inner withSlice
}

type list struct { // want `^refs=true$`
	value int
	next  *list
}

type tree struct { // want `^refs=true$`
	children []tree
}

type namedSlice []int // want `^refs=true$`

// Types without references.

type number = int     // want `^refs=false$`
type numbers = [4]int // want `^refs=false$`
type name string      // want `^refs=false$`

type reader interface { // want `^refs=false$`
	Read() int
}

type box[T any] struct { // want `^refs=false$`
	v T
}

type point struct { // want `^refs=false$`
	x, y int
}

type segment struct { // want `^refs=false$`
	a, b  point
	at    time.Time
	to    lockstep.PID[int]
	from  lockstep.Address
	mark  Batch
	arr   [2]point
	iface any
}

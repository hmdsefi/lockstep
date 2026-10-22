package directives

import "context"

func F(m map[string]int) {
	//lockstep:maporder keys sorted below
	for k := range m {
		_ = k
	}
	for k := range m { //lockstep:maporder ok
		_ = k
	}
	/* want DL000 */   //lockstep:maporder
	for k := range m { // want DL006
		_ = k
	}
	// lockstep:maporder spaced
	for k := range m { // want DL006
		_ = k
	}
	/* want DL000 */ //lockstep:sorted x
	_ = len(m)
	/* want DL000 */ //lockstep:maporder nothing here
	_ = len(m)
}

func S() {
	/* want(core) DL000 */ //lockstep:blocking only BlockSender reaches this
	select {}              // want(core) DL005
}

func A(ctx context.Context, f func()) {
	/* want(core) DL000 */            //lockstep:blocking only BlockSender reaches this
	stop := context.AfterFunc(ctx, f) // want(core) DL004
	_ = stop
}

// The analyzer's directives are neither honored nor reported by the lint.
func Z() {
	//lockstep:allow LS100 demo
	//lockstep:immutable
	_ = 0
}

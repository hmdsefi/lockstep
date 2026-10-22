package directives

import "time"

func N(m map[string]map[string]int) {
	for _, v := range m { //lockstep:maporder only the outer order is unused
		for k := range v { // want DL006
			_ = k
		}
	}
}

func R() {
	/* want DL000 */ //lockstep:maporder not a map
	_ = time.Now()   // want DL001
}

func E(m map[string]int) { // want@21 DL000
	if len(m) > 0 {
		_ = m
	} //lockstep:maporder covers only the closing brace
	for k := range m { // want DL006
		_ = k
	}
}

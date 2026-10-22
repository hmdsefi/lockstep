package dl002

import (
	randv1 "math/rand"
	"math/rand/v2"
)

func F() {
	_ = rand.IntN(3)                   // want DL002
	_ = randv1.Intn(3)                 // want DL002
	rand.Shuffle(2, func(i, j int) {}) // want DL002
	_ = rand.N(5)                      // want DL002
	_ = rand.New(rand.NewPCG(1, 2)).IntN(3)
	_ = randv1.New(randv1.NewSource(1)).Intn(3)
}

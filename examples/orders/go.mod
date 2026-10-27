module github.com/hmdsefi/lockstep/examples/orders

go 1.26.0

require (
	github.com/hmdsefi/faultline v0.1.2
	github.com/hmdsefi/lockstep v0.1.0
)

require github.com/hmdsefi/gograph v0.8.2 // indirect

replace github.com/hmdsefi/lockstep => ../..

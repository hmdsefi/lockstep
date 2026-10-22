package dl005

func blocked() {
	select {} // want DL005
}

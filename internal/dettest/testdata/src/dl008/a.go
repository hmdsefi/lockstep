package dl008

import (
	"os"
	"syscall"
)

func F() {
	_ = os.Getenv("X")         // want DL008
	_, _ = os.LookupEnv("X")   // want DL008
	_ = os.Environ()           // want DL008
	_ = os.ExpandEnv("$X")     // want DL008
	_, _ = syscall.Getenv("X") // want DL008
	_ = syscall.Environ()      // want DL008
	_ = os.Getpid()
}

// G uses the os functions that read environment variables to find a directory.
func G() {
	_ = os.TempDir()          // want DL008
	_, _ = os.UserHomeDir()   // want DL008
	_, _ = os.UserCacheDir()  // want DL008
	_, _ = os.UserConfigDir() // want DL008
	homeDir := os.UserHomeDir // want DL008
	_ = homeDir
	_ = os.IsPathSeparator('/')
	_ = os.Getpagesize()
	_ = TempDir()
	_, _ = UserHomeDir()
}

// TempDir and UserHomeDir are named like the os functions, but are not them.
func TempDir() string { return "" }

func UserHomeDir() (string, error) { return "", nil }

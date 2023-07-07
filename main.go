//go:build !test

package main

import "os"

func main() {
	os.Exit(handleExitError(os.Stderr, runApp(os.Stdout)))
}

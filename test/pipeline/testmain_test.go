//go:build integration

package pipeline

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestMain(m *testing.M) {
	// If clp-s is already in $PATH, just run tests.
	if _, err := exec.LookPath("clp-s"); err == nil {
		os.Exit(m.Run())
	}

	// Try to build clp-s using docker/build-clp.sh.
	projectDir := findProjectRoot()
	buildScript := filepath.Join(projectDir, "docker", "build-clp.sh")
	if _, err := os.Stat(buildScript); err != nil {
		fmt.Fprintln(os.Stderr, "clp-s not in $PATH and docker/build-clp.sh not found — skipping e2e tests")
		fmt.Fprintln(os.Stderr, "Install clp-s from https://github.com/y-scope/clp or add it to $PATH")
		os.Exit(0)
	}

	fmt.Fprintln(os.Stderr, "clp-s not in $PATH, building via docker/build-clp.sh...")
	cmd := exec.Command("bash", buildScript)
	cmd.Dir = projectDir
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "build-clp.sh failed: %v\n", err)
		os.Exit(1)
	}

	// Add build output to PATH.
	outDir := filepath.Join(projectDir, "docker", "out")
	os.Setenv("PATH", outDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	if _, err := exec.LookPath("clp-s"); err != nil {
		fmt.Fprintln(os.Stderr, "clp-s still not found after build — check build-clp.sh output")
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// findProjectRoot walks up from the current file's directory to find the project root
// (the directory containing go.mod).
func findProjectRoot() string {
	_, filename, _, _ := runtime.Caller(0)
	dir := filepath.Dir(filename)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	// Fallback: assume we're in test/pipeline/, go up 2 levels.
	dir = filepath.Dir(filename)
	return filepath.Join(dir, "..", "..")
}

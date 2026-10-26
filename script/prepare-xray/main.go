// Prepare the pinned Xray source with targeted UoT and buffered-write fixes.
package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const upstream = "github.com/wyx2685/xray-core@v0.0.0-20260713170150-b17a88f9b46d"

//go:embed runtime-fixes.patch
var patch []byte

func main() {
	if err := prepare(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func prepare() error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	module, err := os.ReadFile(filepath.Join(root, "go.mod"))
	if err != nil || !strings.HasPrefix(strings.ReplaceAll(string(module), "\r\n", "\n"), "module github.com/YeJianbo/ravel\n") {
		return fmt.Errorf("run from the Ravel source root")
	}
	target := filepath.Join(root, ".xray-core")
	marker := upstream + "\n" + fmt.Sprintf("%x\n", sha256.Sum256(patch))
	replaceTarget := false
	if info, err := os.Lstat(target); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected Xray source path")
		}
		old, err := os.ReadFile(filepath.Join(target, ".ravel-patch"))
		if err != nil || !strings.HasPrefix(string(old), upstream+"\n") {
			return fmt.Errorf("refusing to replace unrecognized .xray-core directory")
		}
		if string(old) == marker {
			fmt.Println("Pinned Xray patch already prepared")
			return nil
		}
		replaceTarget = true
	} else if !os.IsNotExist(err) {
		return err
	}
	command := exec.Command("go", "mod", "download", "-json", upstream)
	command.Dir = os.TempDir()
	command.Env = append(os.Environ(), "GOWORK=off")
	output, err := command.Output()
	if err != nil {
		return fmt.Errorf("download pinned Xray: %w", err)
	}
	var source struct {
		Dir   string
		Error string
	}
	if err := json.Unmarshal(output, &source); err != nil {
		return err
	}
	if source.Dir == "" || source.Error != "" {
		return fmt.Errorf("download pinned Xray: %s", source.Error)
	}
	staging, err := os.MkdirTemp(root, ".xray-core-prepare-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	err = filepath.WalkDir(source.Dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source.Dir, path)
		if err != nil {
			return err
		}
		out := filepath.Join(staging, rel)
		if entry.IsDir() {
			return os.MkdirAll(out, 0755)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("unexpected source entry: %s", rel)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(out, data, 0644)
	})
	if err != nil {
		return err
	}
	patchFile := filepath.Join(staging, ".runtime-fixes.patch")
	if err := os.WriteFile(patchFile, patch, 0644); err != nil {
		return err
	}
	for _, arguments := range [][]string{{"apply", "--check", patchFile}, {"apply", patchFile}} {
		command := exec.Command("git", arguments...)
		command.Dir = staging
		command.Env = append(os.Environ(), "GIT_CEILING_DIRECTORIES="+root)
		if output, err := command.CombinedOutput(); err != nil {
			return fmt.Errorf("apply Xray runtime patch: %w: %s", err, output)
		}
	}
	if err := os.Remove(patchFile); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(staging, ".ravel-patch"), []byte(marker), 0644); err != nil {
		return err
	}
	// Keep the last prepared source usable if downloading or applying fails.
	if replaceTarget {
		if err := os.RemoveAll(target); err != nil {
			return err
		}
	}
	if err := os.Rename(staging, target); err != nil {
		return err
	}
	fmt.Println("Prepared pinned Xray with runtime fixes")
	return nil
}

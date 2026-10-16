package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestMetadataReadersRejectOversizedFiles(t *testing.T) {
	t.Setenv("BUNCLOUD_CONFIG_KEY", "")
	t.Setenv("BUNCLOUD_CONFIG_KEY", "")
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxAgentStateFileBytes + 1); err != nil {
		file.Close()
		t.Fatal(err)
	}
	file.Close()
	if _, err := LoadState(path); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized identity state accepted: %v", err)
	}
	controller := &Controller{KeyFile: path, ManagedFile: filepath.Join(dir, "managed")}
	if _, err := controller.readKey(); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized key accepted: %v", err)
	}
	if err := os.Rename(path, controller.applyStatePath()); err != nil {
		t.Fatal(err)
	}
	controller.applyState.Status = "preserved"
	controller.LoadApplyState()
	if controller.currentApplyState().Status != "preserved" {
		t.Fatal("invalid apply state replaced valid in-memory state")
	}
	binary := filepath.Join(dir, "ravel")
	if err := os.Rename(controller.applyStatePath(), pendingRavelUpdatePath(binary)); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadPendingRavelUpdate(binary); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized pending update accepted: %v", err)
	}
	info, err := os.Stat(pendingRavelUpdatePath(binary))
	if err != nil || info.Size() != maxAgentStateFileBytes+1 {
		t.Fatal("rejected metadata file was modified")
	}
}

func TestBoundedIdentityStatePreservesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte(`{"machine_id":7,"panel_url":"https://panel.example","machine_token":"test"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	state, err := LoadState(path)
	if err != nil || state.MachineID != 7 || state.Protocol != protocol || state.SyncInterval != 30 || state.StatusInterval != 5 {
		t.Fatalf("state defaults changed: %#v, %v", state, err)
	}
}

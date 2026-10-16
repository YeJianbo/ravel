package agent

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplaceRavelBinaryKeepsPreviousVersion(t *testing.T) {
	directory := t.TempDir()
	binaryPath := filepath.Join(directory, "ravel")
	if err := os.WriteFile(binaryPath, []byte("old-binary"), 0o755); err != nil {
		t.Fatal(err)
	}

	var archive bytes.Buffer
	writer := zip.NewWriter(&archive)
	entry, err := writer.Create("ravel")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("new-binary")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	if err := replaceRavelBinary(binaryPath, archive.Bytes()); err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(binaryPath)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := os.ReadFile(binaryPath + ".previous")
	if err != nil {
		t.Fatal(err)
	}
	if string(current) != "new-binary" || string(previous) != "old-binary" {
		t.Fatalf("unexpected binaries: current=%q previous=%q", current, previous)
	}
}

func TestPendingRavelUpdateDoesNotOverwriteRollbackBinary(t *testing.T) {
	directory := t.TempDir()
	binaryPath := filepath.Join(directory, "ravel")
	if err := os.WriteFile(binaryPath, []byte("new-binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(binaryPath+".previous", []byte("old-binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := writePendingRavelUpdate(binaryPath, PendingRavelUpdate{TargetVersion: "v1.2.3"}); err != nil {
		t.Fatal(err)
	}

	err := installRavelRelease(context.Background(), githubRelease{TagName: "v1.2.3"}, binaryPath, "v1.2.2", "request-2")
	if err != nil {
		t.Fatal(err)
	}
	previous, err := os.ReadFile(binaryPath + ".previous")
	if err != nil {
		t.Fatal(err)
	}
	if string(previous) != "old-binary" {
		t.Fatalf("rollback binary was overwritten: %q", previous)
	}
}

func TestVerifyReleaseAssetDigest(t *testing.T) {
	data := []byte("verified-release")
	digest := sha256.Sum256(data)
	if err := verifyReleaseAssetDigest(data, "sha256:"+hex.EncodeToString(digest[:])); err != nil {
		t.Fatal(err)
	}
	if err := verifyReleaseAssetDigest([]byte("tampered"), "sha256:"+hex.EncodeToString(digest[:])); err == nil {
		t.Fatal("tampered release was accepted")
	}
}

func TestDownloadReleaseArchiveStreamsAndVerifies(t *testing.T) {
	var payload bytes.Buffer
	zipWriter := zip.NewWriter(&payload)
	entry, err := zipWriter.Create("ravel")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := entry.Write([]byte("new-binary")); err != nil {
		t.Fatal(err)
	}
	if err := zipWriter.Close(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Write(payload.Bytes())
	}))
	defer server.Close()
	directory := t.TempDir()
	digest := sha256.Sum256(payload.Bytes())
	archive, err := downloadReleaseArchive(context.Background(), server.URL, directory, "sha256:"+hex.EncodeToString(digest[:]))
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(archive.Name())
	defer archive.Close()
	info, err := archive.Stat()
	if err != nil {
		t.Fatal(err)
	}
	binaryPath := filepath.Join(directory, "ravel")
	if err := os.WriteFile(binaryPath, []byte("old-binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := replaceRavelBinaryReaderAt(binaryPath, archive, info.Size(), "ravel", nil); err != nil {
		t.Fatal(err)
	}
	current, err := os.ReadFile(binaryPath)
	if err != nil || string(current) != "new-binary" {
		t.Fatalf("unexpected new binary %q: %v", current, err)
	}
	if _, err := downloadReleaseArchive(context.Background(), server.URL, directory, "sha256:"+hex.EncodeToString(make([]byte, sha256.Size))); err == nil {
		t.Fatal("invalid checksum was accepted")
	}
}

func TestRavelVersionOutputMustMatchReleaseTag(t *testing.T) {
	if !ravelVersionOutputMatches("ravel v1.2.3 (Ravel integrated node runtime)", "v1.2.3") {
		t.Fatal("matching Ravel version output was rejected")
	}
	if ravelVersionOutputMatches("ravel v1.2.2 (Ravel integrated node runtime)", "v1.2.3") {
		t.Fatal("mismatched Ravel version output was accepted")
	}
}

func TestCurrentRavelReleaseTargetUsesBuildArchitecture(t *testing.T) {
	previousOS := runtimeGOOS
	previousArch := runtimeGOARCH
	previousSetting := readCurrentBuildSetting
	t.Cleanup(func() {
		runtimeGOOS = previousOS
		runtimeGOARCH = previousArch
		readCurrentBuildSetting = previousSetting
	})

	runtimeGOOS = "linux"
	runtimeGOARCH = "arm"
	readCurrentBuildSetting = func(name string) string {
		if name == "GOARM" {
			return "6"
		}
		return ""
	}
	target, err := currentRavelReleaseTarget()
	if err != nil {
		t.Fatal(err)
	}
	if target.AssetName != "ravel-linux-arm32-v6.zip" || target.BinaryName != "ravel" {
		t.Fatalf("unexpected ARM target: %+v", target)
	}

	runtimeGOARCH = "mipsle"
	readCurrentBuildSetting = func(name string) string {
		if name == "GOMIPS" {
			return "softfloat"
		}
		return ""
	}
	target, err = currentRavelReleaseTarget()
	if err != nil {
		t.Fatal(err)
	}
	if target.AssetName != "ravel-linux-mips32le.zip" || target.BinaryName != "ravel_softfloat" {
		t.Fatalf("unexpected MIPS target: %+v", target)
	}
}

func TestNormalizeAutoUpdateConfig(t *testing.T) {
	config := normalizeAutoUpdateConfig(AutoUpdateConfig{
		Enabled:       true,
		RequestID:     "request-1",
		TargetVersion: "v0.5.4-ravel4",
	})
	if config.IntervalSeconds != 86400 || config.Repo != "YeJianbo/ravel" {
		t.Fatalf("unexpected normalized config: %+v", config)
	}
	if config.RequestID != "request-1" || config.TargetVersion != "v0.5.4-ravel4" {
		t.Fatalf("manual update request was not preserved: %+v", config)
	}
	if normalized := normalizeAutoUpdateConfig(AutoUpdateConfig{Repo: " yejianbo/RAVEL "}); normalized.Repo != trustedRavelUpdateRepo {
		t.Fatalf("official repository was not normalized: %+v", normalized)
	}
}

func TestRavelUpdatesRejectUntrustedRepositoryBeforeHTTP(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	previous := githubAPIBaseURL
	githubAPIBaseURL = server.URL
	t.Cleanup(func() { githubAPIBaseURL = previous })
	for _, repo := range []string{"untrusted/ravel", "YeJianbo/ravel/../../untrusted", "YeJianbo/v2node", "YeJianbo/ravel-gost"} {
		config := AutoUpdateConfig{Enabled: true, Repo: repo, RequestID: "request-1", TargetVersion: "v0.5.6-ravel1"}
		for _, update := range []func(context.Context, AutoUpdateConfig, string, string) (string, bool, error){
			CheckAndInstallRavelUpdate, CheckAndInstallRequestedRavelUpdate,
		} {
			_, installed, err := update(context.Background(), config, "v0.5.5-ravel1", filepath.Join(t.TempDir(), "ravel"))
			if installed || err == nil || !strings.Contains(err.Error(), "not locally trusted") {
				t.Fatalf("untrusted repository was not rejected: installed=%v error=%v", installed, err)
			}
		}
	}
	if requests != 0 {
		t.Fatalf("untrusted repository reached the HTTP client: %d requests", requests)
	}
}

func TestRequestedRavelUpdateRejectsDowngradeAndWrongRelease(t *testing.T) {
	requests := 0
	tag := "v0.5.4-ravel4"
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests++
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"tag_name":"` + tag + `","assets":[]}`))
	}))
	defer server.Close()
	previous := githubAPIBaseURL
	githubAPIBaseURL = server.URL
	t.Cleanup(func() { githubAPIBaseURL = previous })
	for _, target := range []string{"v0.5.4-ravel4", "invalid-version", "", "v0.5.6-ravel1"} {
		config := AutoUpdateConfig{Repo: trustedRavelUpdateRepo, RequestID: "request-1", TargetVersion: target}
		before := requests
		if target == "v0.5.6-ravel1" {
			tag = "v0.5.7-ravel1"
		}
		_, installed, err := CheckAndInstallRequestedRavelUpdate(context.Background(), config, "v0.5.5-ravel1", filepath.Join(t.TempDir(), "ravel"))
		if installed || err == nil {
			t.Fatalf("invalid update was not rejected: target=%s installed=%v error=%v", target, installed, err)
		}
		if (target == "v0.5.4-ravel4" || target == "invalid-version") && requests != before {
			t.Fatal("invalid explicit version reached the HTTP client")
		}
	}
	if err := validateRequestedRavelVersion("dev", "v0.5.6-ravel1"); err != nil {
		t.Fatalf("upgrade from a development build was rejected: %v", err)
	}
}

func TestFetchGitHubReleaseByTag(t *testing.T) {
	requestedPath := ""
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestedPath = request.URL.Path
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"tag_name":"v0.5.4-ravel4","assets":[]}`))
	}))
	defer server.Close()

	previousAPIBaseURL := githubAPIBaseURL
	githubAPIBaseURL = server.URL
	t.Cleanup(func() { githubAPIBaseURL = previousAPIBaseURL })

	release, err := fetchGitHubRelease(context.Background(), "YeJianbo/ravel", "v0.5.4-ravel4")
	if err != nil {
		t.Fatal(err)
	}
	if release.TagName != "v0.5.4-ravel4" {
		t.Fatalf("unexpected release: %+v", release)
	}
	if requestedPath != "/repos/YeJianbo/ravel/releases/tags/v0.5.4-ravel4" {
		t.Fatalf("unexpected release path: %s", requestedPath)
	}
}

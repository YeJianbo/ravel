package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/YeJianbo/ravel/common/crypt"
	"github.com/YeJianbo/ravel/node"
	log "github.com/sirupsen/logrus"
)

const configPath = "/api/v1/server/machine/ravelConfig"
const statusPath = "/api/v1/server/machine/push"
const restartAckPath = "/api/v1/server/machine/restartAck"
const updateAckPath = "/api/v1/server/machine/updateAck"

var errConfigChangedDuringSync = errors.New("local configuration changed or became unreadable during sync; retry the merge")

const (
	managedMetadataField      = "_BunCloudManagedNodes"
	maxAgentConfigFileBytes   = 16 << 20
	maxManagedIdentitiesBytes = 4 << 20
	maxAgentStateFileBytes    = 1 << 20
	maxAgentKeyFileBytes      = 4 << 10
)

type NodeConfig struct {
	APIHost string `json:"ApiHost"`
	NodeID  int    `json:"NodeID"`
	APIKey  string `json:"ApiKey"`
	Timeout int    `json:"Timeout"`
}

type ConfigResponse struct {
	Data           *[]NodeConfig `json:"data"`
	ConfigRevision string        `json:"config_revision"`
	ConfigSchema   int           `json:"config_schema"`
	Authoritative  bool          `json:"authoritative"`
	RestartToken   string        `json:"restart_node_token"`
	Probe          *ProbeConfig  `json:"probe"`
}

type ProbeConfig struct {
	AutoUpdate     AutoUpdateConfig     `json:"auto_update"`
	Relay          *RelayConfig         `json:"relay"`
	NetworkQuality NetworkQualityConfig `json:"network_quality"`
	RuntimeTask    *RuntimeTask         `json:"runtime_task"`
}

type ConfigApplyState struct {
	DesiredRevision string `json:"desired_revision"`
	AppliedRevision string `json:"applied_revision"`
	Status          string `json:"status"`
	Error           string `json:"error,omitempty"`
	UpdatedAt       int64  `json:"updated_at"`
}

type Controller struct {
	Client              *Client
	ConfigFile          string
	KeyFile             string
	ManagedFile         string
	Relay               *RelayManager
	qualityMu           sync.RWMutex
	quality             NetworkQualityConfig
	updateMu            sync.RWMutex
	autoUpdate          AutoUpdateConfig
	applyMu             sync.RWMutex
	applyState          ConfigApplyState
	runtimeMu           sync.RWMutex
	runtimeTask         *RuntimeTask
	streamMu            sync.RWMutex
	runtimeStream       *RuntimeStreamConfig
	runtimeStreamCursor string
}

func LoadState(path string) (State, error) {
	var state State
	raw, err := readBoundedAgentFile(path, maxAgentStateFileBytes)
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return state, err
	}
	if state.Protocol == "" {
		state.Protocol = protocol
	}
	if state.SyncInterval <= 0 {
		state.SyncInterval = 30
	}
	if state.StatusInterval <= 0 {
		state.StatusInterval = 5
	}
	return state, nil
}

func (c *Controller) Sync() (bool, int, error) {
	return c.SyncContext(context.Background())
}

func (c *Controller) SyncContext(ctx context.Context) (bool, int, error) {
	var response ConfigResponse
	query := url.Values{"t": []string{strconv.FormatInt(time.Now().Unix(), 10)}}
	if err := c.Client.GetContext(ctx, configPath, query, &response); err != nil {
		return false, 0, err
	}
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}
	if response.Data == nil {
		return false, 0, fmt.Errorf("panel response omitted managed node data")
	}
	if response.Probe == nil || response.Probe.Relay == nil {
		return false, len(*response.Data), fmt.Errorf("panel response omitted relay configuration")
	}
	desired := *response.Data
	revision := strings.TrimSpace(response.ConfigRevision)
	if revision == "" {
		revision = configRevision(desired, *response.Probe.Relay)
	}
	revisionChanged := c.setDesiredRevision(revision)

	relayChanged := false
	if c.Relay != nil {
		var err error
		relayChanged, err = c.Relay.SyncContext(ctx, *response.Probe.Relay)
		if err != nil {
			c.MarkConfigApply("failed", err.Error())
			return false, len(desired), fmt.Errorf("apply relay configuration: %w", err)
		}
	}
	c.setNetworkQualityConfig(response.Probe.NetworkQuality)
	c.setAutoUpdateConfig(response.Probe.AutoUpdate)
	c.setRuntimeTask(response.Probe.RuntimeTask)
	nodeConfigChanged, err := c.writeMergedConfig(desired)
	if err != nil {
		if !errors.Is(err, errConfigChangedDuringSync) {
			c.MarkConfigApply("failed", err.Error())
		}
		return false, len(desired), err
	}
	restartToken := response.RestartToken
	restartRequested := restartToken != ""
	if restartRequested {
		var ack map[string]any
		if err := c.Client.PostContext(ctx, restartAckPath, map[string]string{"restart_token": restartToken}, &ack); err != nil {
			return false, len(desired), fmt.Errorf("acknowledge restart: %w", err)
		}
	}
	state := c.currentApplyState()
	if nodeConfigChanged || restartRequested || state.Status == "" {
		c.MarkConfigApply("applying", "")
	} else if relayChanged || revisionChanged ||
		((state.Status == "success" || state.Status == "partial") && state.AppliedRevision != revision) {
		if state.Status == "partial" {
			c.MarkConfigApply("partial", state.Error)
		} else {
			c.MarkConfigApply("success", "")
		}
	}
	return nodeConfigChanged || restartRequested, len(desired), nil
}

func configRevision(nodes []NodeConfig, relay RelayConfig) string {
	raw, _ := json.Marshal(struct {
		Nodes []NodeConfig `json:"nodes"`
		Relay RelayConfig  `json:"relay"`
	}{Nodes: nodes, Relay: relay})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (c *Controller) setRuntimeTask(task *RuntimeTask) {
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	if task == nil {
		c.runtimeTask = nil
		return
	}
	copyTask := *task
	c.runtimeTask = &copyTask
}

func (c *Controller) RuntimeTask() *RuntimeTask {
	c.runtimeMu.RLock()
	defer c.runtimeMu.RUnlock()
	if c.runtimeTask == nil {
		return nil
	}
	copyTask := *c.runtimeTask
	return &copyTask
}

func (c *Controller) applyStatePath() string {
	if strings.TrimSpace(c.ManagedFile) == "" {
		return ""
	}
	return c.ManagedFile + ".apply-state.json"
}

func (c *Controller) LoadApplyState() {
	path := c.applyStatePath()
	if path == "" {
		return
	}
	raw, err := readBoundedAgentFile(path, maxAgentStateFileBytes)
	if err != nil {
		return
	}
	var state ConfigApplyState
	if json.Unmarshal(raw, &state) != nil {
		return
	}
	c.applyMu.Lock()
	c.applyState = state
	c.applyMu.Unlock()
}

func (c *Controller) setDesiredRevision(revision string) bool {
	revision = strings.TrimSpace(revision)
	c.applyMu.Lock()
	changed := c.applyState.DesiredRevision != revision
	c.applyState.DesiredRevision = revision
	c.applyMu.Unlock()
	return changed
}

func (c *Controller) currentApplyState() ConfigApplyState {
	c.applyMu.RLock()
	state := c.applyState
	c.applyMu.RUnlock()
	return state
}

func (c *Controller) MarkConfigApply(status, message string) {
	status = strings.ToLower(strings.TrimSpace(status))
	if status == "" {
		status = "unknown"
	}
	c.applyMu.Lock()
	c.applyState.Status = status
	c.applyState.Error = truncateStatusMessage(message, 255)
	c.applyState.UpdatedAt = time.Now().Unix()
	if status == "success" || status == "partial" {
		c.applyState.AppliedRevision = c.applyState.DesiredRevision
	}
	if status == "success" {
		c.applyState.Error = ""
	}
	state := c.applyState
	c.applyMu.Unlock()
	c.persistApplyState(state)
}

func (c *Controller) ConfigStatus() map[string]any {
	c.applyMu.RLock()
	state := c.applyState
	c.applyMu.RUnlock()
	return map[string]any{
		"config_desired_revision": state.DesiredRevision,
		"config_applied_revision": state.AppliedRevision,
		"config_apply_status":     state.Status,
		"config_apply_error":      state.Error,
		"config_apply_at":         state.UpdatedAt,
	}
}

func (c *Controller) persistApplyState(state ConfigApplyState) {
	path := c.applyStatePath()
	if path == "" {
		return
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err == nil {
		_ = atomicWrite(path, append(raw, '\n'), 0o600)
	}
}

func (c *Controller) LocalNodeCount() (int, error) {
	raw, err := readBoundedAgentFile(c.ConfigFile, maxAgentConfigFileBytes)
	if err != nil {
		return 0, err
	}
	plain, err := c.decrypt(raw)
	if err != nil {
		return 0, err
	}
	var config struct {
		Nodes []NodeConfig `json:"Nodes"`
	}
	if err := json.Unmarshal(plain, &config); err != nil {
		return 0, err
	}
	return len(config.Nodes), nil
}

func (c *Controller) LastGoodConfigPath() string {
	return c.ConfigFile + ".last-good"
}

func (c *Controller) SaveLastGoodConfig() error {
	raw, err := readBoundedAgentFile(c.ConfigFile, maxAgentConfigFileBytes)
	if err != nil {
		return err
	}
	return atomicWrite(c.LastGoodConfigPath(), raw, 0o600)
}

func (c *Controller) RestoreLastGoodConfig() error {
	raw, err := readBoundedAgentFile(c.LastGoodConfigPath(), maxAgentConfigFileBytes)
	if err != nil {
		return err
	}
	return atomicWrite(c.ConfigFile, raw, 0o600)
}

func (c *Controller) runtimeSnapshotPath() string {
	if strings.TrimSpace(c.ManagedFile) == "" {
		return ""
	}
	return c.ManagedFile + ".runtime-cache.enc.json"
}

func (c *Controller) LoadRuntimeSnapshot() (node.RuntimeSnapshot, error) {
	var snapshot node.RuntimeSnapshot
	path := c.runtimeSnapshotPath()
	if path == "" {
		return snapshot, os.ErrNotExist
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return snapshot, err
	}
	plain, err := c.decrypt(raw)
	if err != nil {
		return snapshot, err
	}
	if err := json.Unmarshal(plain, &snapshot); err != nil {
		return snapshot, err
	}
	return snapshot, nil
}

func (c *Controller) SaveRuntimeSnapshot(snapshot node.RuntimeSnapshot) error {
	path := c.runtimeSnapshotPath()
	if path == "" {
		return fmt.Errorf("runtime snapshot path is not configured")
	}
	plain, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	key, err := c.readKey()
	if err != nil {
		return err
	}
	encrypted, err := crypt.EncryptConfig(plain, key)
	if err != nil {
		return err
	}
	return atomicWrite(path, encrypted, 0o600)
}

func (c *Controller) setAutoUpdateConfig(config AutoUpdateConfig) {
	config = normalizeAutoUpdateConfig(config)
	c.updateMu.Lock()
	c.autoUpdate = config
	c.updateMu.Unlock()
}

func (c *Controller) AutoUpdateConfig() AutoUpdateConfig {
	c.updateMu.RLock()
	defer c.updateMu.RUnlock()
	return c.autoUpdate
}

func (c *Controller) AcknowledgeUpdate(config AutoUpdateConfig, status, installedVersion, message string) error {
	return c.AcknowledgeUpdateContext(context.Background(), config, status, installedVersion, message)
}

func (c *Controller) AcknowledgeUpdateContext(ctx context.Context, config AutoUpdateConfig, status, installedVersion, message string) error {
	var response map[string]any
	return c.Client.PostContext(ctx, updateAckPath, map[string]string{
		"request_id":        config.RequestID,
		"status":            status,
		"target_version":    config.TargetVersion,
		"installed_version": installedVersion,
		"error":             truncateStatusMessage(message, 255),
	}, &response)
}

func (c *Controller) ClearUpdateRequest(requestID string) {
	c.updateMu.Lock()
	if c.autoUpdate.RequestID == requestID {
		c.autoUpdate.RequestID = ""
		c.autoUpdate.TargetVersion = ""
		c.autoUpdate.RequestedAt = 0
	}
	c.updateMu.Unlock()
}

func truncateStatusMessage(message string, limit int) string {
	message = strings.TrimSpace(message)
	runes := []rune(message)
	if len(runes) <= limit {
		return message
	}
	return string(runes[:limit])
}

func (c *Controller) setNetworkQualityConfig(config NetworkQualityConfig) {
	config = normalizeNetworkQualityConfig(config)
	c.qualityMu.Lock()
	c.quality = config
	c.qualityMu.Unlock()
}

func (c *Controller) NetworkQualityConfig() NetworkQualityConfig {
	c.qualityMu.RLock()
	defer c.qualityMu.RUnlock()
	config := c.quality
	config.Targets = append([]NetworkQualityTarget(nil), config.Targets...)
	return config
}

func (c *Controller) PushStatus(status map[string]any) error {
	return c.PushStatusContext(context.Background(), status)
}

func (c *Controller) PushStatusContext(ctx context.Context, status map[string]any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	for key, value := range c.Client.PublicAddresses() {
		status[key] = value
	}
	if c.Relay != nil {
		for key, value := range c.Relay.Status() {
			status[key] = value
		}
	}
	for key, value := range c.ConfigStatus() {
		status[key] = value
	}
	streamChunk := c.collectRuntimeStream(ctx)
	if streamChunk != nil {
		status["runtime_stream"] = streamChunk
	}
	var response RuntimeStatusResponse
	if err := c.Client.PostContext(ctx, statusPath, status, &response); err != nil {
		return err
	}
	if streamChunk != nil {
		c.acknowledgeRuntimeStream(streamChunk)
	}
	c.setRuntimeStream(response.RuntimeStream)
	return nil
}

func (c *Controller) writeMergedConfig(desired []NodeConfig) (bool, error) {
	existing := map[string]json.RawMessage{}
	var existingNodes []NodeConfig
	originalConfig, configReadErr := readBoundedAgentFile(c.ConfigFile, maxAgentConfigFileBytes)
	if configReadErr == nil {
		plain, decodeErr := c.decrypt(originalConfig)
		if decodeErr != nil {
			return false, decodeErr
		}
		if err := json.Unmarshal(plain, &existing); err != nil {
			return false, fmt.Errorf("decode existing config: %w", err)
		}
		if existing == nil {
			return false, fmt.Errorf("existing config must be a JSON object")
		}
		if nodeRaw := existing["Nodes"]; len(nodeRaw) > 0 {
			if err := json.Unmarshal(nodeRaw, &existingNodes); err != nil {
				return false, fmt.Errorf("decode existing nodes: %w", err)
			}
		}
	} else if !os.IsNotExist(configReadErr) {
		return false, fmt.Errorf("read existing config: %w", configReadErr)
	}

	previousManaged := map[string]bool{}
	originalManaged, managedReadErr := readBoundedAgentFile(c.ManagedFile, maxManagedIdentitiesBytes)
	identityRaw, embeddedManaged := existing[managedMetadataField]
	if !embeddedManaged {
		if managedReadErr != nil && !os.IsNotExist(managedReadErr) {
			return false, fmt.Errorf("read managed identities: %w", managedReadErr)
		}
		if managedReadErr == nil {
			identityRaw = originalManaged
		}
	}
	if len(identityRaw) > 0 {
		var identities []string
		if err := json.Unmarshal(identityRaw, &identities); err != nil {
			return false, fmt.Errorf("decode managed identities: %w", err)
		}
		if identities == nil {
			return false, fmt.Errorf("managed identities must be a JSON array")
		}
		for _, identity := range identities {
			previousManaged[identity] = true
		}
	}
	legacyOwners := map[string]string{}
	for _, node := range existingNodes {
		legacy := strconv.Itoa(node.NodeID)
		if node.NodeID <= 0 || !previousManaged[legacy] {
			continue
		}
		identity := nodeIdentity(node)
		if previous, found := legacyOwners[legacy]; found && previous != identity {
			return false, fmt.Errorf("legacy managed node %s belongs to multiple panel addresses; resolve its ownership before syncing", legacy)
		}
		legacyOwners[legacy] = identity
	}
	for legacy, identity := range legacyOwners {
		delete(previousManaged, legacy)
		previousManaged[identity] = true
	}

	manual := make([]NodeConfig, 0, len(existingNodes))
	seen := map[string]bool{}
	for _, node := range existingNodes {
		identity := nodeIdentity(node)
		if identity == "" || previousManaged[identity] || seen[identity] {
			continue
		}
		seen[identity] = true
		manual = append(manual, node)
	}
	merged := append([]NodeConfig{}, manual...)
	managedIdentities := make([]string, 0, len(desired))
	legacyManagedIdentities := make([]string, 0, len(desired))
	for _, node := range desired {
		identity := nodeIdentity(node)
		if identity == "" || seen[identity] {
			continue
		}
		if node.Timeout <= 0 {
			node.Timeout = 15
		}
		seen[identity] = true
		merged = append(merged, node)
		managedIdentities = append(managedIdentities, identity)
		legacyManagedIdentities = append(legacyManagedIdentities, legacyNodeIdentity(node))
	}

	_, hasLog := existing["Log"]
	if !hasLog {
		existing["Log"] = json.RawMessage(`{"Level":"warning","Output":"","Access":"none"}`)
	}
	nodeRaw, err := json.Marshal(merged)
	if err != nil {
		return false, err
	}
	changed := !hasLog || !jsonEqual(existing["Nodes"], nodeRaw)
	managedRaw, err := json.MarshalIndent(managedIdentities, "", "  ")
	if err != nil {
		return false, err
	}
	if len(managedRaw) > maxManagedIdentitiesBytes {
		return false, fmt.Errorf("merged managed identities exceed %d bytes", maxManagedIdentitiesBytes)
	}
	legacyManagedRaw, err := json.MarshalIndent(legacyManagedIdentities, "", "  ")
	if err != nil {
		return false, err
	}
	metadataChanged := !jsonEqual(existing[managedMetadataField], managedRaw)
	configChanged := changed || metadataChanged
	var plain []byte
	if configChanged {
		existing["Nodes"] = nodeRaw
		existing[managedMetadataField] = managedRaw
		plain, err = json.MarshalIndent(existing, "", "  ")
		if err != nil {
			return false, err
		}
	}
	managedChanged := !jsonEqual(originalManaged, legacyManagedRaw)

	// Detect edits made while preparing the merge, without decrypting both files again.
	if !sameOptionalFileContent(c.ConfigFile, originalConfig, configReadErr) ||
		(!embeddedManaged && !sameOptionalFileContent(c.ManagedFile, originalManaged, managedReadErr)) {
		return false, errConfigChangedDuringSync
	}
	if configChanged {
		key, err := c.readKey()
		if err != nil {
			return false, err
		}
		encrypted, err := crypt.EncryptConfig(plain, key)
		if err != nil {
			return false, err
		}
		if len(encrypted) > maxAgentConfigFileBytes {
			return false, fmt.Errorf("merged encrypted configuration exceeds %d bytes", maxAgentConfigFileBytes)
		}
		if err := atomicWrite(c.ConfigFile, encrypted, 0o600); err != nil {
			return false, err
		}
	}
	if managedChanged {
		// This sidecar is a legacy mirror; ownership is committed with Nodes above.
		if sameOptionalFileContent(c.ManagedFile, originalManaged, managedReadErr) {
			if err := atomicWrite(c.ManagedFile, legacyManagedRaw, 0o600); err != nil {
				log.WithError(err).Warn("Could not update legacy managed-node mirror; encrypted configuration remains authoritative")
			}
		} else {
			log.Warn("Legacy managed-node mirror changed during sync; preserving it")
		}
	}
	return changed, nil
}

func readBoundedAgentFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("agent state must be a regular file")
	}
	if info.Size() > limit {
		return nil, fmt.Errorf("agent state file exceeds %d bytes", limit)
	}
	// One extra byte detects growth without ReadAll's expanding intermediate buffers.
	data := make([]byte, info.Size()+1)
	n, err := io.ReadFull(file, data)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	}
	if n == len(data) {
		return nil, errConfigChangedDuringSync
	}
	return data[:n], nil
}

func sameOptionalFileContent(path string, original []byte, originalErr error) bool {
	file, err := os.Open(path)
	if err != nil {
		return os.IsNotExist(originalErr) && os.IsNotExist(err)
	}
	defer file.Close()
	if originalErr != nil {
		return false
	}
	var buffer [32 << 10]byte
	offset := 0
	for {
		n, err := file.Read(buffer[:])
		if offset+n > len(original) || !bytes.Equal(buffer[:n], original[offset:offset+n]) {
			return false
		}
		offset += n
		if err != nil {
			return err == io.EOF && offset == len(original)
		}
	}
}

func (c *Controller) readKey() ([]byte, error) {
	if key, err := crypt.ReadConfigKeyFromEnv(); err == nil {
		return key, nil
	}
	raw, err := readBoundedAgentFile(c.KeyFile, maxAgentKeyFileBytes)
	if err != nil {
		return nil, fmt.Errorf("read config key: %w", err)
	}
	return crypt.ParseConfigKey(string(raw))
}

func (c *Controller) decrypt(raw []byte) ([]byte, error) {
	var envelope crypt.ConfigEnvelope
	if json.Unmarshal(raw, &envelope) != nil || envelope.Ciphertext == "" {
		return raw, nil
	}
	key, err := c.readKey()
	if err != nil {
		return nil, err
	}
	return crypt.DecryptConfig(raw, key)
}

func nodeIdentity(node NodeConfig) string {
	host := strings.TrimRight(strings.TrimSpace(node.APIHost), "/")
	if parsed, err := url.Parse(host); err == nil && parsed.Scheme != "" && parsed.Host != "" {
		parsed.Scheme = strings.ToLower(parsed.Scheme)
		parsed.Host = strings.ToLower(parsed.Host)
		host = parsed.String()
	}
	if host == "" && node.NodeID <= 0 {
		return ""
	}
	return host + "#" + strconv.Itoa(node.NodeID)
}

func legacyNodeIdentity(node NodeConfig) string {
	if node.NodeID > 0 {
		return strconv.Itoa(node.NodeID)
	}
	return nodeIdentity(node)
}

func jsonEqual(a, b []byte) bool {
	var left, right any
	return json.Unmarshal(a, &left) == nil && json.Unmarshal(b, &right) == nil && reflect.DeepEqual(left, right)
}

func atomicWrite(path string, data []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".ravel-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

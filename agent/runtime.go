package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	runtimeAckPath        = "/api/v1/server/machine/runtimeTaskAck"
	runtimeMaxOutput      = 32 * 1024
	runtimeStreamMaxChunk = 8 * 1024
)

type RuntimeStatusResponse struct {
	RuntimeStream *RuntimeStreamConfig `json:"runtime_stream"`
}

type RuntimeStreamConfig struct {
	SessionID string `json:"session_id"`
	Service   string `json:"service"`
	ExpiresAt int64  `json:"expires_at"`
}

type RuntimeStreamChunk struct {
	SessionID   string `json:"session_id"`
	Service     string `json:"service"`
	Logs        string `json:"logs,omitempty"`
	Error       string `json:"error,omitempty"`
	CollectedAt int64  `json:"collected_at"`
	nextCursor  string
}

type RuntimeTask struct {
	TaskID    string `json:"task_id"`
	Service   string `json:"service"`
	Action    string `json:"action"`
	Lines     int    `json:"lines"`
	CreatedAt int64  `json:"created_at"`
	ExpiresAt int64  `json:"expires_at"`
}

type RuntimeResult struct {
	TaskID        string `json:"task_id"`
	Service       string `json:"service"`
	Action        string `json:"action"`
	Status        string `json:"status"`
	Message       string `json:"message"`
	ServiceStatus string `json:"service_status,omitempty"`
	ExitCode      int    `json:"exit_code"`
	Logs          string `json:"logs,omitempty"`
	ExecutedAt    int64  `json:"executed_at"`
}

func (c *Controller) setRuntimeStream(config *RuntimeStreamConfig) {
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	if config == nil || strings.TrimSpace(config.SessionID) == "" || config.ExpiresAt <= time.Now().Unix() {
		c.runtimeStream = nil
		c.runtimeStreamCursor = ""
		return
	}
	normalizedService := normalizeRuntimeService(config.Service)
	if normalizedService == "" {
		c.runtimeStream = nil
		c.runtimeStreamCursor = ""
		return
	}
	if c.runtimeStream == nil || c.runtimeStream.SessionID != config.SessionID {
		c.runtimeStreamCursor = ""
	}
	copyConfig := *config
	copyConfig.Service = normalizedService
	c.runtimeStream = &copyConfig
}

func (c *Controller) currentRuntimeStream() (*RuntimeStreamConfig, string) {
	c.streamMu.RLock()
	defer c.streamMu.RUnlock()
	if c.runtimeStream == nil {
		return nil, ""
	}
	copyConfig := *c.runtimeStream
	return &copyConfig, c.runtimeStreamCursor
}

func (c *Controller) collectRuntimeStream(ctx context.Context) *RuntimeStreamChunk {
	if ctx.Err() != nil {
		return nil
	}
	config, cursor := c.currentRuntimeStream()
	if config == nil {
		return nil
	}
	if config.ExpiresAt <= time.Now().Unix() {
		c.setRuntimeStream(nil)
		return nil
	}

	if runtimeOpenRC() || runtimeHasFileLog(config.Service) {
		logs, next, err := readRuntimeFile(runtimeLogPath(config.Service), cursor, runtimeStreamMaxChunk)
		chunk := &RuntimeStreamChunk{SessionID: config.SessionID, Service: config.Service, Logs: logs, CollectedAt: time.Now().Unix(), nextCursor: next}
		if err != nil {
			chunk.Error = err.Error()
		}
		return chunk
	}
	unit := runtimeServiceUnitContext(ctx, config.Service)
	args := []string{"-u", unit, "--no-pager", "-o", "short-iso", "--show-cursor"}
	if cursor == "" {
		args = append(args, "-n", "80")
	} else {
		args = append(args, "--after-cursor", cursor)
	}
	output, _, err := runRuntimeCommandContext(ctx, 5*time.Second, "journalctl", args...)
	if ctx.Err() != nil {
		return nil
	}
	logs, nextCursor := parseRuntimeJournalOutput(output)
	chunk := &RuntimeStreamChunk{
		SessionID:   config.SessionID,
		Service:     config.Service,
		Logs:        tailRuntimeLogOutput(logs, runtimeStreamMaxChunk),
		CollectedAt: time.Now().Unix(),
		nextCursor:  nextCursor,
	}
	if err != nil {
		chunk.Error = "读取实时日志失败: " + commandErrorMessage(err)
		chunk.nextCursor = ""
	}
	return chunk
}

func (c *Controller) acknowledgeRuntimeStream(chunk *RuntimeStreamChunk) {
	if chunk == nil || chunk.nextCursor == "" {
		return
	}
	c.streamMu.Lock()
	defer c.streamMu.Unlock()
	if c.runtimeStream != nil && c.runtimeStream.SessionID == chunk.SessionID {
		c.runtimeStreamCursor = chunk.nextCursor
	}
}

func parseRuntimeJournalOutput(output string) (string, string) {
	lines := strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n")
	logLines := make([]string, 0, len(lines))
	cursor := ""
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "-- cursor:") {
			cursor = strings.TrimSpace(strings.TrimPrefix(trimmed, "-- cursor:"))
			continue
		}
		if trimmed == "-- No entries --" {
			continue
		}
		logLines = append(logLines, line)
	}
	return strings.TrimSpace(strings.Join(logLines, "\n")), cursor
}

func tailRuntimeLogOutput(output string, limit int) string {
	output = strings.TrimSpace(output)
	if len(output) <= limit {
		return output
	}
	tail := output[len(output)-limit:]
	if newline := strings.IndexByte(tail, '\n'); newline >= 0 {
		tail = tail[newline+1:]
	}
	return strings.TrimSpace(tail)
}

func (c *Controller) ProcessRuntimeTask(requestReload func() error) error {
	return c.ProcessRuntimeTaskContext(context.Background(), requestReload)
}

func (c *Controller) ProcessRuntimeTaskContext(ctx context.Context, requestReload func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	task := c.RuntimeTask()
	if task == nil || strings.TrimSpace(task.TaskID) == "" {
		return nil
	}

	resultPath := filepath.Join(filepath.Dir(c.ManagedFile), "runtime-task-result.json")
	result, loaded, err := loadRuntimeResult(resultPath, task.TaskID)
	if err != nil {
		return fmt.Errorf("read persisted runtime task result: %w", err)
	}
	if !loaded {
		result = executeRuntimeTaskWithRelay(ctx, *task, requestReload, c.Relay)
		raw, err := json.Marshal(result)
		if err != nil {
			return err
		}
		if err := atomicWrite(resultPath, raw, 0o600); err != nil {
			return fmt.Errorf("persist runtime task result: %w", err)
		}
	}

	var response map[string]any
	if err := c.Client.PostContext(ctx, runtimeAckPath, result, &response); err != nil {
		return fmt.Errorf("acknowledge runtime task: %w", err)
	}
	return nil
}

func loadRuntimeResult(path, taskID string) (RuntimeResult, bool, error) {
	var result RuntimeResult
	file, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return RuntimeResult{}, false, nil
		}
		return RuntimeResult{}, false, err
	}
	defer file.Close()
	const maxResultBytes = 256 * 1024
	raw, err := io.ReadAll(io.LimitReader(file, maxResultBytes+1))
	if err != nil {
		return RuntimeResult{}, false, err
	}
	if len(raw) > maxResultBytes {
		return RuntimeResult{}, false, errors.New("runtime result exceeds 256 KiB")
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return RuntimeResult{}, false, fmt.Errorf("invalid runtime result JSON: %w", err)
	}
	return result, result.TaskID == taskID, nil
}

func executeRuntimeTask(task RuntimeTask, requestReload func() error) RuntimeResult {
	return executeRuntimeTaskContext(context.Background(), task, requestReload)
}

func executeRuntimeTaskContext(ctx context.Context, task RuntimeTask, requestReload func() error) RuntimeResult {
	return executeRuntimeTaskWithRelay(ctx, task, requestReload, nil)
}

func executeRuntimeTaskWithRelay(ctx context.Context, task RuntimeTask, requestReload func() error, relay *RelayManager) RuntimeResult {
	result := RuntimeResult{
		TaskID:     strings.TrimSpace(task.TaskID),
		Service:    normalizeRuntimeService(task.Service),
		Action:     strings.ToLower(strings.TrimSpace(task.Action)),
		Status:     "failed",
		ExitCode:   -1,
		ExecutedAt: time.Now().Unix(),
	}
	if task.ExpiresAt > 0 && task.ExpiresAt <= time.Now().Unix() {
		result.Message = "运行任务已过期"
		return result
	}
	if result.Service == "" {
		result.Message = "不支持的服务"
		return result
	}
	if task.ExpiresAt > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, time.Unix(task.ExpiresAt, 0))
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		result.Message = "运行任务已取消: " + commandErrorMessage(err)
		return result
	}
	if result.Service == "gost" && relay != nil {
		return executeManagedRelayTask(ctx, task, result, relay)
	}

	lines := task.Lines
	if lines < 50 || lines > 300 {
		lines = 200
	}
	unit := runtimeServiceUnitContext(ctx, result.Service)
	result.ServiceStatus = serviceActiveStateContext(ctx, unit)
	if err := ctx.Err(); err != nil {
		result.Message = "运行任务已取消: " + commandErrorMessage(err)
		return result
	}

	switch result.Action {
	case "logs":
		output, exitCode, err := readRuntimeLogsContext(ctx, result.Service, lines)
		result.ExitCode = exitCode
		result.Logs = tailOutput(output, runtimeMaxOutput)
		if err != nil {
			result.Message = "读取运行日志失败: " + commandErrorMessage(err)
			return result
		}
		result.Status = "success"
		result.Message = fmt.Sprintf("已读取最近 %d 行日志", lines)
		return result
	case "status":
		var output string
		var exitCode int
		var err error
		if runtimeOpenRC() {
			output, exitCode, err = runtimeServiceCommandContext(ctx, result.Service, "status")
		} else {
			output, exitCode, err = runRuntimeCommandContext(ctx, 10*time.Second, "systemctl", "show", unit, "--no-pager", "--property=ActiveState,SubState,Result,ExecMainStatus,ExecMainStartTimestamp")
		}
		result.ExitCode = exitCode
		result.Logs = tailOutput(output, runtimeMaxOutput)
		if err != nil {
			result.Message = "读取服务状态失败: " + commandErrorMessage(err)
			return result
		}
		result.Status = "success"
		result.Message = "服务状态读取完成"
		return result
	case "reload", "restart":
		if result.Service == "ravel" {
			if err := ctx.Err(); err != nil {
				result.Message = "运行任务已取消: " + commandErrorMessage(err)
				return result
			}
			if requestReload == nil {
				result.Message = "Ravel 重载通道不可用"
				return result
			}
			if err := requestReload(); err != nil {
				result.Message = "Ravel 重载请求失败: " + err.Error()
				return result
			}
			result.Status = "success"
			result.ExitCode = 0
			result.Message = "Ravel 已接收配置重载请求"
			return result
		}
	case "start", "stop":
		if result.Service != "gost" {
			result.Message = "控制进程不允许远程启动或停止"
			return result
		}
	default:
		result.Message = "不支持的运行操作"
		return result
	}

	output, exitCode, err := runtimeServiceCommandContext(ctx, result.Service, result.Action)
	result.ExitCode = exitCode
	result.Logs = tailOutput(output, runtimeMaxOutput)
	result.ServiceStatus = serviceActiveStateContext(ctx, unit)
	if err != nil {
		result.Message = fmt.Sprintf("%s %s 失败: %s", result.Service, result.Action, commandErrorMessage(err))
		return result
	}
	result.Status = "success"
	result.Message = fmt.Sprintf("%s %s 完成", result.Service, result.Action)
	return result
}

func executeManagedRelayTask(ctx context.Context, task RuntimeTask, result RuntimeResult, relay *RelayManager) RuntimeResult {
	if result.Action == "logs" {
		lines := task.Lines
		if lines < 50 || lines > 300 {
			lines = 200
		}
		logService := "ravel"
		if !relay.useEmbedded() && (runtimeOpenRC() || runtimeHasFileLog("gost")) {
			logService = "gost"
		}
		output, code, err := readRuntimeLogsContext(ctx, logService, lines)
		result.ExitCode = code
		result.Logs = tailOutput(output, runtimeMaxOutput)
		if err != nil {
			result.Message = "读取运行日志失败: " + commandErrorMessage(err)
		} else {
			result.Status = "success"
			result.Message = fmt.Sprintf("GOST 由 Ravel 管理，已读取 %s 运行日志", logService)
		}
	} else if result.Action == "status" {
		result.Status = "success"
		result.ExitCode = 0
		result.Message = "转发器状态读取完成"
	} else if err := relay.ControlContext(ctx, result.Action); err != nil {
		result.Message = fmt.Sprintf("gost %s 失败: %s", result.Action, commandErrorMessage(err))
	} else {
		result.Status = "success"
		result.ExitCode = 0
		result.Message = fmt.Sprintf("gost %s 完成", result.Action)
	}
	state := relay.Status()
	result.ServiceStatus, _ = state["gost_status"].(string)
	if result.Action == "status" {
		raw, _ := json.Marshal(map[string]any{
			"status": state["gost_status"], "version": state["gost_version"],
			"rules": state["gost_rule_count"], "error": state["gost_error"],
		})
		result.Logs = string(raw)
	}
	return result
}

func normalizeRuntimeService(service string) string {
	switch strings.ToLower(strings.TrimSpace(service)) {
	case "ravel":
		return "ravel"
	case "gost":
		return "gost"
	default:
		return ""
	}
}

func runtimeServiceUnit(service string) string {
	return runtimeServiceUnitContext(context.Background(), service)
}

func runtimeServiceUnitContext(ctx context.Context, service string) string {
	if runtimeOpenRC() {
		return runtimeOpenRCService(service) + ".service"
	}
	if service == "gost" {
		return "gost.service"
	}
	return "ravel.service"
}

func serviceUnitExists(unit string) bool {
	return serviceUnitExistsContext(context.Background(), unit)
}

func serviceUnitExistsContext(ctx context.Context, unit string) bool {
	output, _, err := runRuntimeCommandContext(ctx, 5*time.Second, "systemctl", "show", unit, "--property=LoadState", "--value")
	return err == nil && strings.TrimSpace(output) == "loaded"
}

func serviceActiveState(unit string) string {
	return serviceActiveStateContext(context.Background(), unit)
}

func serviceActiveStateContext(ctx context.Context, unit string) string {
	if runtimeOpenRC() {
		_, _, err := runRuntimeCommandContext(ctx, 5*time.Second, "rc-service", strings.TrimSuffix(unit, ".service"), "status")
		if ctx.Err() != nil {
			return "unknown"
		}
		if err == nil {
			return "active"
		}
		return "inactive"
	}
	output, _, err := runRuntimeCommandContext(ctx, 5*time.Second, "systemctl", "is-active", unit)
	if ctx.Err() != nil {
		return "unknown"
	}
	state := strings.TrimSpace(output)
	if state == "" && err != nil {
		return "unknown"
	}
	return state
}

type runtimeOutputBuffer struct {
	mu    sync.Mutex
	data  []byte
	start int
	size  int
}

func (b *runtimeOutputBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p) == 0 {
		return 0, nil
	}
	if b.data == nil {
		b.data = make([]byte, runtimeMaxOutput)
	}
	if len(p) >= runtimeMaxOutput {
		copy(b.data, p[len(p)-runtimeMaxOutput:])
		b.start = 0
		b.size = runtimeMaxOutput
	} else {
		end := (b.start + b.size) % runtimeMaxOutput
		written := copy(b.data[end:], p)
		copy(b.data, p[written:])
		overflow := max(0, b.size+len(p)-runtimeMaxOutput)
		b.start = (b.start + overflow) % runtimeMaxOutput
		b.size = min(runtimeMaxOutput, b.size+len(p))
	}
	return len(p), nil
}

func (b *runtimeOutputBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.size == 0 {
		return ""
	}
	end := b.start + b.size
	if end <= runtimeMaxOutput {
		return string(b.data[b.start:end])
	}
	var output strings.Builder
	output.Grow(b.size)
	output.Write(b.data[b.start:])
	output.Write(b.data[:end-runtimeMaxOutput])
	return output.String()
}

func runRuntimeCommand(timeout time.Duration, name string, args ...string) (string, int, error) {
	return runRuntimeCommandContext(context.Background(), timeout, name, args...)
}

func runRuntimeCommandContext(parent context.Context, timeout time.Duration, name string, args ...string) (string, int, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	command := exec.CommandContext(ctx, name, args...)
	command.WaitDelay = 2 * time.Second
	command.Env = append(command.Environ(), "LC_ALL=C", "LANG=C")
	var output runtimeOutputBuffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	exitCode := 0
	if err != nil {
		exitCode = -1
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			exitCode = exitError.ExitCode()
		}
	}
	if ctx.Err() == context.DeadlineExceeded {
		return output.String(), exitCode, fmt.Errorf("command timed out")
	}
	if ctx.Err() != nil {
		return output.String(), exitCode, ctx.Err()
	}
	return output.String(), exitCode, err
}

func commandErrorMessage(err error) string {
	if err == nil {
		return ""
	}
	return strings.TrimSpace(err.Error())
}

func tailOutput(output string, limit int) string {
	if len(output) <= limit {
		return strings.TrimSpace(output)
	}
	return strings.TrimSpace(output[len(output)-limit:])
}

package cmd

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"

	"github.com/YeJianbo/ravel/agent"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var (
	ravelStateFile   string
	ravelConfigFile  string
	ravelKeyFile     string
	ravelManagedFile string
)

var ravelCommand = cobra.Command{
	Use:     "run",
	Aliases: []string{"ravel"},
	Short:   "Run the integrated Ravel control plane and node runtime",
	Args:    cobra.NoArgs,
	Run:     ravelHandle,
}

func init() {
	ravelCommand.Flags().StringVar(&ravelStateFile, "state", "/etc/ravel/state.json", "Ravel state path")
	ravelCommand.Flags().StringVar(&ravelConfigFile, "config", "/etc/ravel/config.enc.json", "encrypted node config path")
	ravelCommand.Flags().StringVar(&ravelKeyFile, "key", "/etc/ravel/config.key", "config key path")
	ravelCommand.Flags().StringVar(&ravelManagedFile, "managed", "/etc/ravel/managed-nodes.json", "managed node state path")
	command.AddCommand(&ravelCommand)
}

func ravelHandle(_ *cobra.Command, _ []string) {
	agent.SetRuntimeVersion(version)
	agent.StartNetworkRateSampler()
	state, err := agent.LoadState(ravelStateFile)
	if err != nil {
		log.WithError(err).Fatal("load Ravel state failed")
	}
	client, err := agent.NewClient(state)
	if err != nil {
		log.WithError(err).Fatal("initialize Ravel client failed")
	}
	controller := &agent.Controller{
		Client:      client,
		ConfigFile:  ravelConfigFile,
		KeyFile:     ravelKeyFile,
		ManagedFile: ravelManagedFile,
		Relay:       agent.NewRelayManager("/usr/local/ravel/gost", "/etc/.buncloud-agent/relay.json"),
	}
	defer controller.Relay.Shutdown()
	controller.LoadApplyState()
	snapshot, snapshotErr := controller.LoadRuntimeSnapshot()
	if snapshotErr != nil && !os.IsNotExist(snapshotErr) {
		log.WithError(snapshotErr).Warn("load cached Ravel runtime data failed")
	}
	changed, nodeCount, err := controller.Sync()
	if err != nil {
		log.WithError(err).Warn("initial Ravel sync failed; starting from the last local runtime state")
		nodeCount, err = controller.LocalNodeCount()
		if err != nil {
			log.WithError(err).Warn("current local Ravel configuration is unusable; restoring last-good configuration")
			if restoreErr := controller.RestoreLastGoodConfig(); restoreErr != nil {
				log.WithError(restoreErr).Fatal("no usable local Ravel configuration")
			}
			nodeCount, err = controller.LocalNodeCount()
			if err != nil {
				log.WithError(err).Fatal("restored last-good Ravel configuration is unusable")
			}
		}
		if relayErr := controller.Relay.StartExisting(); relayErr != nil {
			log.WithError(relayErr).Warn("start cached relay configuration failed")
		}
	} else {
		log.Infof("Ravel synchronized %d managed nodes", nodeCount)
	}
	_ = changed

	reloadCh := make(chan struct{}, 1)
	var runtimeReady atomic.Bool
	binaryPath, binaryPathErr := os.Executable()
	if binaryPathErr != nil {
		log.WithError(binaryPathErr).Warn("resolve Ravel executable failed")
	}
	var pendingUpdate agent.PendingRavelUpdate
	if binaryPathErr == nil {
		loadedUpdate, loadErr := agent.LoadPendingRavelUpdate(binaryPath)
		pendingUpdate = loadedUpdate
		if loadErr != nil && !os.IsNotExist(loadErr) {
			log.WithError(loadErr).Warn("load pending Ravel update state failed")
		}
	}
	loopContext, stopLoop := context.WithCancel(context.Background())
	defer stopLoop()
	go runRavelLoop(loopContext, controller, state, reloadCh, nodeCount, &runtimeReady)
	err = runServerWithApply(
		ravelConfigFile,
		reloadCh,
		false,
		controller.LastGoodConfigPath(),
		snapshot,
		func(report runtimeApplyReport) {
			runtimeReady.Store(report.ActiveNodes > 0 || report.RequestedNodes == 0)
			agent.SetNodeHealth(buildRuntimeNodeHealth(report))
			controller.MarkConfigApply(report.Status, report.Error)
			if runtimeReady.Load() && pendingUpdate.TargetVersion == version && binaryPathErr == nil {
				if err := agent.CompletePendingRavelUpdate(binaryPath); err != nil {
					log.WithError(err).Warn("complete pending Ravel update failed")
				} else {
					pendingUpdate = agent.PendingRavelUpdate{}
				}
			}
			if report.Status == "success" || report.Status == "partial" {
				if err := controller.SaveRuntimeSnapshot(report.Snapshot); err != nil {
					log.WithError(err).Warn("save cached Ravel runtime data failed")
				}
			}
			if report.Status == "success" {
				if err := controller.SaveLastGoodConfig(); err != nil {
					log.WithError(err).Warn("save last-good Ravel configuration failed")
				}
			}
		},
	)
	stopLoop()
	if err != nil {
		log.WithError(err).Error("Ravel runtime stopped")
		if pendingUpdate.TargetVersion == version && binaryPathErr == nil {
			if rollbackErr := agent.RestorePreviousRavelBinary(binaryPath); rollbackErr != nil {
				log.WithError(rollbackErr).Error("restore previous Ravel binary failed")
				return
			}
			if restartErr := scheduleRavelRestart(); restartErr != nil {
				log.WithError(restartErr).Error("restart previous Ravel binary failed")
			}
		}
	}
}

func buildRuntimeNodeHealth(report runtimeApplyReport) []agent.NodeHealth {
	checkedAt := time.Now().Unix()
	health := make([]agent.NodeHealth, 0, len(report.Snapshot.Nodes)+len(report.Failures))
	seen := make(map[int]bool, len(report.Snapshot.Nodes))
	for _, item := range report.Snapshot.Nodes {
		protocol := ""
		serverPort := 0
		if item.Info != nil {
			protocol = strings.ToLower(strings.TrimSpace(item.Info.Type))
			if item.Info.Common != nil {
				serverPort = item.Info.Common.ServerPort
			}
		}
		health = append(health, agent.NodeHealth{
			NodeID:     item.NodeID,
			Protocol:   protocol,
			Status:     "running",
			ListenOK:   true,
			CheckedAt:  checkedAt,
			ServerPort: serverPort,
		})
		seen[item.NodeID] = true
	}
	for _, failure := range report.Failures {
		if seen[failure.NodeID] {
			continue
		}
		health = append(health, agent.NodeHealth{
			NodeID:    failure.NodeID,
			Status:    "failed",
			ListenOK:  false,
			Message:   failure.Stage + ": " + failure.Error,
			CheckedAt: checkedAt,
		})
	}
	return health
}

func runRavelLoop(ctx context.Context, controller *agent.Controller, state agent.State, reloadCh chan<- struct{}, nodeCount int, runtimeReady *atomic.Bool) {
	syncTicker := time.NewTicker(time.Duration(state.SyncInterval) * time.Second)
	statusTicker := time.NewTicker(time.Duration(state.StatusInterval) * time.Second)
	defer syncTicker.Stop()
	defer statusTicker.Stop()
	var qualityRunning atomic.Bool
	var updateRunning atomic.Bool
	var syncRunning atomic.Bool
	var statusRunning atomic.Bool
	var managedNodeCount atomic.Int64
	managedNodeCount.Store(int64(nodeCount))
	var currentUpdateVersion atomic.Value
	currentUpdateVersion.Store(version)
	qualityNext := time.Now()
	qualityFingerprint := ""
	var qualityCancel context.CancelFunc
	defer func() {
		if qualityCancel != nil {
			qualityCancel()
		}
	}()
	updateNext := time.Now()
	stopCancelledUpdate := func(binaryPath string, updated bool) bool {
		if ctx.Err() == nil {
			return false
		}
		if updated {
			if err := agent.RestorePreviousRavelBinary(binaryPath); err != nil {
				log.WithError(err).Error("restore Ravel update after loop cancellation failed")
			}
		}
		return true
	}
	maybeRunNetworkQuality := func() {
		config := controller.NetworkQualityConfig()
		fingerprint := agent.NetworkQualityFingerprint(config)
		if fingerprint != qualityFingerprint {
			if qualityCancel != nil {
				qualityCancel()
				qualityCancel = nil
			}
			qualityFingerprint = fingerprint
			qualityNext = time.Now()
		}
		if !config.Enabled {
			qualityNext = time.Time{}
			return
		}
		if !qualityNext.IsZero() && time.Now().Before(qualityNext) {
			return
		}
		if !qualityRunning.CompareAndSwap(false, true) {
			return
		}
		qualityNext = time.Now().Add(agent.NetworkQualityInterval(config))
		probeContext, cancelProbe := context.WithCancel(ctx)
		qualityCancel = cancelProbe
		go func() {
			defer cancelProbe()
			defer qualityRunning.Store(false)
			if err := controller.ProbeAndPushNetworkQualityContext(probeContext); err != nil && probeContext.Err() == nil {
				log.WithError(err).Warn("Ravel network quality report failed")
			}
		}()
	}
	maybeRunAutoUpdate := func() {
		if ctx.Err() != nil {
			return
		}
		if runtimeReady != nil && !runtimeReady.Load() {
			return
		}
		config := controller.AutoUpdateConfig()
		if config.RequestID != "" {
			if !updateRunning.CompareAndSwap(false, true) {
				return
			}
			go func(request agent.AutoUpdateConfig) {
				defer updateRunning.Store(false)
				binaryPath, err := os.Executable()
				if err != nil {
					log.WithError(err).Warn("resolve Ravel executable failed")
					return
				}
				updateCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
				defer cancel()
				currentVersion, _ := currentUpdateVersion.Load().(string)
				target, updated, updateErr := agent.CheckAndInstallRequestedRavelUpdate(
					updateCtx,
					request,
					currentVersion,
					binaryPath,
				)
				if stopCancelledUpdate(binaryPath, updated) {
					return
				}
				if updateErr != nil {
					log.WithError(updateErr).Warn("Ravel manual update failed")
					if err := controller.AcknowledgeUpdateContext(ctx, request, "failed", currentVersion, updateErr.Error()); err != nil {
						log.WithError(err).Warn("acknowledge Ravel manual update failure failed")
					} else {
						controller.ClearUpdateRequest(request.RequestID)
					}
					return
				}
				if !updated {
					installedVersion := currentVersion
					if target != "" {
						installedVersion = target
					}
					if err := controller.AcknowledgeUpdateContext(ctx, request, "success", installedVersion, ""); err != nil {
						log.WithError(err).Warn("acknowledge Ravel manual update failed")
					} else {
						controller.ClearUpdateRequest(request.RequestID)
					}
					return
				}
				log.Infof("Ravel manually updated from %s to %s; scheduling service restart", currentVersion, target)
				if err := scheduleRavelRestart(); err != nil {
					log.WithError(err).Error("schedule Ravel restart failed")
					rollbackErr := agent.RestorePreviousRavelBinary(binaryPath)
					message := err.Error()
					if rollbackErr != nil {
						message += "; rollback failed: " + rollbackErr.Error()
					}
					if ackErr := controller.AcknowledgeUpdateContext(ctx, request, "failed", currentVersion, message); ackErr != nil {
						log.WithError(ackErr).Warn("acknowledge Ravel restart failure failed")
					} else {
						controller.ClearUpdateRequest(request.RequestID)
					}
				}
			}(config)
			return
		}
		if !config.Enabled {
			updateNext = time.Time{}
			return
		}
		if updateNext.IsZero() {
			updateNext = time.Now()
		}
		if time.Now().Before(updateNext) || !updateRunning.CompareAndSwap(false, true) {
			return
		}
		updateNext = time.Now().Add(agent.AutoUpdateInterval(config))
		go func() {
			defer updateRunning.Store(false)
			binaryPath, err := os.Executable()
			if err != nil {
				log.WithError(err).Warn("resolve Ravel executable failed")
				return
			}
			updateCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			currentVersion, _ := currentUpdateVersion.Load().(string)
			latest, updated, err := agent.CheckAndInstallRavelUpdate(updateCtx, config, currentVersion, binaryPath)
			if stopCancelledUpdate(binaryPath, updated) {
				return
			}
			if err != nil {
				log.WithError(err).Warn("Ravel auto update check failed")
				return
			}
			if !updated {
				log.Debugf("Ravel is current at %s", latest)
				return
			}
			log.Infof("Ravel updated from %s to %s; scheduling service restart", currentVersion, latest)
			if err := scheduleRavelRestart(); err != nil {
				log.WithError(err).Error("schedule Ravel restart failed")
				if rollbackErr := agent.RestorePreviousRavelBinary(binaryPath); rollbackErr != nil {
					log.WithError(rollbackErr).Error("restore previous Ravel binary failed")
				}
			}
		}()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-syncTicker.C:
			if ctx.Err() != nil || !syncRunning.CompareAndSwap(false, true) {
				continue
			}
			go func() {
				defer syncRunning.Store(false)
				if ctx.Err() != nil {
					return
				}
				changed, count, err := controller.SyncContext(ctx)
				if err != nil {
					if ctx.Err() == nil {
						log.WithError(err).Warn("Ravel sync failed")
					}
					return
				}
				managedNodeCount.Store(int64(count))
				if ctx.Err() != nil {
					return
				}
				if changed {
					select {
					case reloadCh <- struct{}{}:
					default:
					}
				}
				if err := controller.ProcessRuntimeTaskContext(ctx, func() error {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					select {
					case reloadCh <- struct{}{}:
					default:
					}
					return nil
				}); err != nil && ctx.Err() == nil {
					log.WithError(err).Warn("Ravel runtime task failed")
				}
			}()
		case <-statusTicker.C:
			if ctx.Err() != nil {
				return
			}
			if statusRunning.CompareAndSwap(false, true) {
				go func() {
					defer statusRunning.Store(false)
					if ctx.Err() != nil {
						return
					}
					if err := controller.PushStatusContext(ctx, agent.CollectStatus(int(managedNodeCount.Load()))); err != nil && ctx.Err() == nil {
						log.WithError(err).Warn("Ravel status report failed")
					}
				}()
			}
			maybeRunNetworkQuality()
			maybeRunAutoUpdate()
		}
	}
}

func scheduleRavelRestart() error {
	command := exec.Command("sh", "-c", "(sleep 2; if command -v systemctl >/dev/null 2>&1; then systemctl restart ravel; else service ravel restart; fi) >/dev/null 2>&1 &")
	return command.Run()
}

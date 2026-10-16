package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	panel "github.com/YeJianbo/ravel/api/v2board"
	"github.com/YeJianbo/ravel/common/domaintraffic"
	"github.com/YeJianbo/ravel/common/task"
	"github.com/YeJianbo/ravel/conf"
	"github.com/YeJianbo/ravel/core"
	"github.com/YeJianbo/ravel/limiter"
)

type Controller struct {
	runtimeMu               sync.RWMutex
	snapshotMu              sync.RWMutex
	closed                  bool
	server                  *core.V2Core
	apiClient               *panel.Client
	tag                     string
	limiter                 *limiter.Limiter
	userList                []panel.UserInfo
	aliveMap                map[int]int
	conf                    *conf.NodeConfig
	info                    *panel.NodeInfo
	nodeInfoMonitorPeriodic *task.Task
	userReportPeriodic      *task.Task
	renewCertPeriodic       *task.Task
	hasCachedRuntime        bool
	pendingTraffic          []panel.UserTraffic
	trafficBatch            *panel.UserTrafficBatch
	pendingDomains          *panel.DomainTrafficBatch
	unreportedDomains       []domaintraffic.Record
	lastDomainReport        time.Time
}

// NewController return a Node controller with default parameters.
func NewController(api *panel.Client, conf *conf.NodeConfig, info *panel.NodeInfo) *Controller {
	controller := &Controller{
		apiClient: api,
		info:      info,
		conf:      conf,
	}
	return controller
}

func (c *Controller) SetCachedRuntime(users []panel.UserInfo, alive map[int]int) {
	if len(users) == 0 {
		return
	}
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	c.userList = append([]panel.UserInfo(nil), users...)
	c.aliveMap = make(map[int]int, len(alive))
	for userID, count := range alive {
		c.aliveMap[userID] = count
	}
	c.hasCachedRuntime = true
}

func (c *Controller) SetPendingTraffic(batch *panel.UserTrafficBatch, pending []panel.UserTraffic) {
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	c.trafficBatch = copyTrafficBatch(batch)
	c.pendingTraffic = append([]panel.UserTraffic(nil), pending...)
}

func copyTrafficBatch(batch *panel.UserTrafficBatch) *panel.UserTrafficBatch {
	if batch == nil {
		return nil
	}
	return &panel.UserTrafficBatch{ID: batch.ID, Records: append([]panel.UserTraffic(nil), batch.Records...)}
}

func (c *Controller) setPendingDomains(batch *panel.DomainTrafficBatch) {
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	c.pendingDomains = copyDomainTrafficBatch(batch)
}

func copyDomainTrafficBatch(batch *panel.DomainTrafficBatch) *panel.DomainTrafficBatch {
	if batch == nil {
		return nil
	}
	copy := *batch
	copy.Records = append([]domaintraffic.Record(nil), batch.Records...)
	return &copy
}

func (c *Controller) setUnreportedDomains(records []domaintraffic.Record) {
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	c.unreportedDomains = append([]domaintraffic.Record(nil), records...)
}

func (c *Controller) restoreDomainCounters() {
	domaintraffic.Enable(c.tag, c.info.Common.BaseConfig != nil && c.info.Common.BaseConfig.DomainTrafficEnable)
	domaintraffic.Restore(c.tag, c.unreportedDomains)
	c.unreportedDomains = nil
}

// Start implement the Start() function of the service interface
func (c *Controller) Start(x *core.V2Core) error {
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	// Init Core
	c.server = x
	var err error
	// First fetch Node Info
	node := c.info
	if node == nil {
		c.info, err = c.apiClient.GetNodeInfo(context.Background())
		if err != nil {
			return fmt.Errorf("get node info error: %s", err)
		}
		node = c.info
	}
	if node.PullInterval <= 0 || node.PushInterval <= 0 {
		return errors.New("node pull and push intervals must be positive")
	}
	// Update user
	users, userErr := c.apiClient.GetUserList(context.Background())
	if userErr == nil {
		if len(users) == 0 && node.Type != "ravel" {
			return errors.New("add users error: not have any user")
		}
		c.userList = users
		c.hasCachedRuntime = false
	} else if !c.hasCachedRuntime {
		return fmt.Errorf("get user list error: %s", userErr)
	} else {
		log.WithError(userErr).Warnf("using cached users for %s", c.conf.APIHost)
	}
	if len(c.userList) == 0 && node.Type != "ravel" {
		return errors.New("add users error: not have any user")
	}
	alive, aliveErr := c.apiClient.GetUserAlive(context.Background())
	if aliveErr == nil {
		c.aliveMap = alive
	} else if c.aliveMap == nil {
		return fmt.Errorf("failed to get user alive list: %s", aliveErr)
	} else {
		log.WithError(aliveErr).Warnf("using cached user limits for %s", c.conf.APIHost)
	}
	c.tag = node.Tag

	// add limiter
	// The limiter removes users from its map; keep the recovery snapshot separate.
	l := limiter.AddLimiter(c.info.Type, c.tag, c.userList, maps.Clone(c.aliveMap))
	c.limiter = l
	if node.Security == panel.Tls {
		err = c.requestCert()
		if err != nil {
			limiter.DeleteLimiter(c.tag)
			c.limiter = nil
			return fmt.Errorf("request cert error: %s", err)
		}
	}
	// Add new tag
	err = c.server.AddNode(c.tag, node)
	if err != nil {
		limiter.DeleteLimiter(c.tag)
		c.limiter = nil
		return fmt.Errorf("add new node error: %s", err)
	}
	added, err := c.server.AddUsers(&core.AddUsersParams{
		Tag:      c.tag,
		Users:    c.userList,
		NodeInfo: node,
	})
	if err != nil {
		_ = c.server.DelNode(c.tag)
		limiter.DeleteLimiter(c.tag)
		c.limiter = nil
		return fmt.Errorf("add users error: %s", err)
	}
	log.WithField("tag", c.tag).Infof("Added %d new users", added)
	c.info = node
	c.restoreDomainCounters()
	c.startTasks(node)
	return nil
}

func (c *Controller) RuntimeSnapshot() CachedNode {
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	c.snapshotMu.RLock()
	defer c.snapshotMu.RUnlock()
	alive := make(map[int]int, len(c.aliveMap))
	for userID, count := range c.aliveMap {
		alive[userID] = count
	}
	return CachedNode{
		APIHost:           c.conf.APIHost,
		NodeID:            c.conf.NodeID,
		Info:              c.info,
		Users:             append([]panel.UserInfo(nil), c.userList...),
		Alive:             alive,
		TrafficBatch:      copyTrafficBatch(c.trafficBatch),
		PendingTraffic:    append([]panel.UserTraffic(nil), c.pendingTraffic...),
		PendingDomains:    copyDomainTrafficBatch(c.pendingDomains),
		UnreportedDomains: append([]domaintraffic.Record(nil), c.unreportedDomains...),
	}
}

// Close implement the Close() function of the service interface
func (c *Controller) Close() error {
	if c.nodeInfoMonitorPeriodic != nil {
		c.nodeInfoMonitorPeriodic.Close()
	}
	if c.userReportPeriodic != nil {
		c.userReportPeriodic.Close()
	}
	if c.renewCertPeriodic != nil {
		c.renewCertPeriodic.Close()
	}
	// Cancel first, then let core-facing callbacks finish before removing resources.
	c.runtimeMu.Lock()
	defer c.runtimeMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	defer c.apiClient.CloseIdleConnections()
	defer func() {
		// DelNodeWithTraffic has stopped in-flight I/O before this final drain.
		c.unreportedDomains = append(c.unreportedDomains, domaintraffic.Drain(c.tag)...)
		domaintraffic.Remove(c.tag)
	}()
	if c.tag != "" {
		defer limiter.DeleteLimiter(c.tag)
	}
	if c.server == nil || c.tag == "" {
		return nil
	}
	traffic, err := c.server.DelNodeWithTraffic(c.tag)
	c.pendingTraffic = mergePendingTraffic(c.pendingTraffic, traffic)
	if err != nil {
		return fmt.Errorf("del node error: %s", err)
	}
	return nil
}

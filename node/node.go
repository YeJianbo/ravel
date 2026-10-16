package node

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	log "github.com/sirupsen/logrus"
	panel "github.com/YeJianbo/ravel/api/v2board"
	"github.com/YeJianbo/ravel/common/domaintraffic"
	"github.com/YeJianbo/ravel/conf"
	"github.com/YeJianbo/ravel/core"
)

type Node struct {
	controllers []*Controller
	NodeInfos   []*panel.NodeInfo
	failures    []NodeFailure
	requested   int
	retained    map[string]CachedNode
}

type NodeFailure struct {
	APIHost string `json:"api_host"`
	NodeID  int    `json:"node_id"`
	Stage   string `json:"stage"`
	Error   string `json:"error"`
}

type RuntimeSnapshot struct {
	Nodes []CachedNode `json:"nodes"`
}

type CachedNode struct {
	APIHost           string                    `json:"api_host"`
	NodeID            int                       `json:"node_id"`
	Info              *panel.NodeInfo           `json:"info"`
	Users             []panel.UserInfo          `json:"users"`
	Alive             map[int]int               `json:"alive"`
	TrafficBatch      *panel.UserTrafficBatch   `json:"traffic_batch,omitempty"`
	PendingTraffic    []panel.UserTraffic       `json:"pending_traffic,omitempty"`
	PendingDomains    *panel.DomainTrafficBatch `json:"pending_domains,omitempty"`
	UnreportedDomains []domaintraffic.Record    `json:"unreported_domains,omitempty"`
}

func New(nodes []conf.NodeConfig) (*Node, error) {
	return NewWithSnapshot(nodes, RuntimeSnapshot{})
}

func NewWithSnapshot(nodes []conf.NodeConfig, snapshot RuntimeSnapshot) (*Node, error) {
	n := &Node{
		controllers: make([]*Controller, 0, len(nodes)),
		NodeInfos:   make([]*panel.NodeInfo, 0, len(nodes)),
		failures:    make([]NodeFailure, 0),
		requested:   len(nodes),
		retained:    make(map[string]CachedNode),
	}
	cached := make(map[string]CachedNode, len(snapshot.Nodes))
	for _, item := range snapshot.Nodes {
		cached[nodeConfigIdentity(item.APIHost, item.NodeID)] = item
	}
	seenTags := make(map[string]struct{}, len(nodes))
	for _, nodeConfig := range nodes {
		identity := nodeConfigIdentity(nodeConfig.APIHost, nodeConfig.NodeID)
		item, ok := cached[identity]
		if !ok {
			item = CachedNode{APIHost: nodeConfig.APIHost, NodeID: nodeConfig.NodeID}
		}
		// Failed preparation still needs the old runtime's final pending batches.
		n.retained[identity] = copyCachedNode(item)
		p, err := panel.New(&nodeConfig)
		if err != nil {
			n.addFailure(nodeConfig, "initialize", err)
			continue
		}
		info, err := p.GetNodeInfo(context.Background())
		if err != nil {
			item, ok := cached[nodeConfigIdentity(nodeConfig.APIHost, nodeConfig.NodeID)]
			if !ok || item.Info == nil {
				p.CloseIdleConnections()
				n.addFailure(nodeConfig, "fetch", err)
				continue
			}
			info = item.Info
			log.WithError(err).Warnf("using cached node runtime data for %s#%d", nodeConfig.APIHost, nodeConfig.NodeID)
		}
		if info == nil || strings.TrimSpace(info.Tag) == "" {
			p.CloseIdleConnections()
			n.addFailure(nodeConfig, "initialize", fmt.Errorf("node runtime tag is empty"))
			continue
		}
		if _, exists := seenTags[info.Tag]; exists {
			p.CloseIdleConnections()
			n.addFailure(nodeConfig, "initialize", fmt.Errorf("duplicate node runtime tag %q", info.Tag))
			continue
		}
		seenTags[info.Tag] = struct{}{}
		controller := NewController(p, &nodeConfig, info)
		if item, ok := cached[nodeConfigIdentity(nodeConfig.APIHost, nodeConfig.NodeID)]; ok {
			controller.SetCachedRuntime(item.Users, item.Alive)
			controller.SetPendingTraffic(item.TrafficBatch, item.PendingTraffic)
			controller.setPendingDomains(item.PendingDomains)
			controller.setUnreportedDomains(item.UnreportedDomains)
		}
		n.controllers = append(n.controllers, controller)
		n.NodeInfos = append(n.NodeInfos, info)
		delete(n.retained, identity)
	}
	for _, controller := range n.controllers {
		delete(n.retained, nodeConfigIdentity(controller.conf.APIHost, controller.conf.NodeID))
	}
	return n, nil
}

func (n *Node) Start(_ []conf.NodeConfig, core *core.V2Core) error {
	active := make([]*Controller, 0, len(n.controllers))
	for _, controller := range n.controllers {
		err := controller.Start(core)
		if err != nil {
			controller.apiClient.CloseIdleConnections()
			n.addFailure(*controller.conf, "start", err)
			if n.retained == nil {
				n.retained = make(map[string]CachedNode)
			}
			n.retained[nodeConfigIdentity(controller.conf.APIHost, controller.conf.NodeID)] = controller.RuntimeSnapshot()
			continue
		}
		active = append(active, controller)
	}
	n.controllers = active
	if len(n.failures) == 0 {
		return nil
	}
	return fmt.Errorf("%d of %d managed nodes failed: %s", len(n.failures), n.requested, n.FailureMessage())
}

func (n *Node) Close() error {
	_, err := n.close(false)
	return err
}

func (n *Node) CloseWithSnapshot() (RuntimeSnapshot, error) {
	return n.close(true)
}

func (n *Node) close(capture bool) (RuntimeSnapshot, error) {
	var snapshot RuntimeSnapshot
	if capture {
		snapshot = n.retainedSnapshot()
	}
	var result error
	for _, c := range n.controllers {
		if err := c.Close(); err != nil {
			log.Errorf("close controller failed: %v", err)
			result = errors.Join(result, err)
		}
		if capture {
			snapshot.Nodes = append(snapshot.Nodes, c.RuntimeSnapshot())
		}
	}
	n.controllers = nil
	return snapshot, result
}

func (n *Node) RestorePendingTraffic(snapshot RuntimeSnapshot) {
	cached := make(map[string]CachedNode, len(snapshot.Nodes))
	for _, item := range snapshot.Nodes {
		cached[nodeConfigIdentity(item.APIHost, item.NodeID)] = item
	}
	for identity := range n.retained {
		if item, ok := cached[identity]; ok {
			n.retained[identity] = copyCachedNode(item)
		}
	}
	for _, controller := range n.controllers {
		if item, ok := cached[nodeConfigIdentity(controller.conf.APIHost, controller.conf.NodeID)]; ok {
			controller.SetPendingTraffic(item.TrafficBatch, item.PendingTraffic)
			controller.setPendingDomains(item.PendingDomains)
			controller.setUnreportedDomains(item.UnreportedDomains)
		}
	}
}

func (n *Node) ActiveCount() int {
	return len(n.controllers)
}

func (n *Node) RequestedCount() int {
	return n.requested
}

func (n *Node) Failures() []NodeFailure {
	return append([]NodeFailure(nil), n.failures...)
}

func (n *Node) RuntimeSnapshot() RuntimeSnapshot {
	snapshot := n.retainedSnapshot()
	for _, controller := range n.controllers {
		snapshot.Nodes = append(snapshot.Nodes, controller.RuntimeSnapshot())
	}
	return snapshot
}

func (n *Node) retainedSnapshot() RuntimeSnapshot {
	snapshot := RuntimeSnapshot{Nodes: make([]CachedNode, 0, len(n.controllers)+len(n.retained))}
	for _, item := range n.retained {
		snapshot.Nodes = append(snapshot.Nodes, copyCachedNode(item))
	}
	return snapshot
}

func copyCachedNode(item CachedNode) CachedNode {
	item.Users = slices.Clone(item.Users)
	item.Alive = maps.Clone(item.Alive)
	item.TrafficBatch = copyTrafficBatch(item.TrafficBatch)
	item.PendingTraffic = slices.Clone(item.PendingTraffic)
	item.PendingDomains = copyDomainTrafficBatch(item.PendingDomains)
	item.UnreportedDomains = slices.Clone(item.UnreportedDomains)
	return item
}

func (n *Node) FailureMessage() string {
	messages := make([]string, 0, len(n.failures))
	for _, failure := range n.failures {
		messages = append(messages, fmt.Sprintf("%s#%d %s: %s", failure.APIHost, failure.NodeID, failure.Stage, failure.Error))
	}
	return strings.Join(messages, "; ")
}

func (n *Node) addFailure(config conf.NodeConfig, stage string, err error) {
	n.failures = append(n.failures, NodeFailure{
		APIHost: config.APIHost,
		NodeID:  config.NodeID,
		Stage:   stage,
		Error:   err.Error(),
	})
}

func nodeConfigIdentity(apiHost string, nodeID int) string {
	return strings.TrimRight(strings.ToLower(strings.TrimSpace(apiHost)), "/") + "#" + fmt.Sprint(nodeID)
}

package node

import (
	"net/http"
	"net/http/httptest"
	"testing"

	panel "github.com/YeJianbo/ravel/api/v2board"
	"github.com/YeJianbo/ravel/common/domaintraffic"
	"github.com/YeJianbo/ravel/conf"
)

func TestFailedStartRetainsPendingSnapshot(t *testing.T) {
	config := &conf.NodeConfig{APIHost: "http://panel.invalid", NodeID: 7}
	c := NewController(nil, config, &panel.NodeInfo{}) // Invalid intervals fail before network I/O.
	c.SetPendingTraffic(&panel.UserTrafficBatch{ID: "traffic-retry", Records: []panel.UserTraffic{{UID: 7, Upload: 31}}}, nil)
	c.setPendingDomains(&panel.DomainTrafficBatch{ID: "domain-retry", RecordedAt: 123, Records: []domaintraffic.Record{{Domain: "old.example", Download: 47}}})
	c.setUnreportedDomains([]domaintraffic.Record{{Domain: "fresh.example", Upload: 5}})
	n := &Node{controllers: []*Controller{c}, requested: 1}
	if n.Start(nil, nil) == nil || n.ActiveCount() != 0 || n.RequestedCount() != 1 {
		t.Fatal("failed node counted as active")
	}
	snapshot := n.RuntimeSnapshot()
	if len(snapshot.Nodes) != 1 {
		t.Fatal("failed controller snapshot lost")
	}
	item := snapshot.Nodes[0]
	if item.TrafficBatch.ID != "traffic-retry" || item.PendingDomains.ID != "domain-retry" || item.PendingDomains.RecordedAt != 123 || item.UnreportedDomains[0].Upload != 5 {
		t.Fatalf("pending batches changed: %+v", item)
	}
	item.TrafficBatch.Records[0].Upload = 999
	item.PendingDomains.Records[0].Download = 999
	item.UnreportedDomains[0].Upload = 999
	closed, err := n.CloseWithSnapshot()
	if err != nil || len(closed.Nodes) != 1 {
		t.Fatalf("close lost retained data: %+v %v", closed, err)
	}
	item = closed.Nodes[0]
	if item.TrafficBatch.Records[0].Upload != 31 || item.PendingDomains.Records[0].Download != 47 || item.UnreportedDomains[0].Upload != 5 {
		t.Fatal("snapshot aliases retained batches")
	}
}

func TestFailedPreparationUsesFinalSnapshotAndDropsRemovedNodes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	defer server.Close()
	zero := 0
	config := conf.NodeConfig{APIHost: server.URL, NodeID: 7, RetryCount: &zero}
	old := CachedNode{APIHost: server.URL, NodeID: 7, TrafficBatch: &panel.UserTrafficBatch{ID: "already-acknowledged"}}
	n, err := NewWithSnapshot([]conf.NodeConfig{config}, RuntimeSnapshot{Nodes: []CachedNode{old, {APIHost: server.URL, NodeID: 8}}})
	if err != nil || n.ActiveCount() != 0 || len(n.RuntimeSnapshot().Nodes) != 1 {
		t.Fatalf("unexpected prepared state: %v", err)
	}
	final := CachedNode{APIHost: server.URL, NodeID: 7, PendingTraffic: []panel.UserTraffic{{UID: 7, Upload: 9}}}
	n.RestorePendingTraffic(RuntimeSnapshot{Nodes: []CachedNode{final}})
	final.PendingTraffic[0].Upload = 999
	closed, err := n.CloseWithSnapshot()
	if err != nil || len(closed.Nodes) != 1 || closed.Nodes[0].TrafficBatch != nil || closed.Nodes[0].PendingTraffic[0].Upload != 9 {
		t.Fatalf("stale batch restored or final counters lost: %+v %v", closed, err)
	}
}

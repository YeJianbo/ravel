package node

import (
	"testing"

	panel "github.com/YeJianbo/ravel/api/v2board"
	"github.com/YeJianbo/ravel/common/domaintraffic"
	"github.com/YeJianbo/ravel/conf"
)

func TestDomainSnapshotKeepsUnsubmittedCountersSeparateFromRetryBatch(t *testing.T) {
	tag := t.Name()
	domaintraffic.Enable(tag, true)
	t.Cleanup(func() { domaintraffic.Remove(tag) })
	counter, release := domaintraffic.Acquire(tag, "fresh.example")
	counter.Add(3, 4)
	release()
	configuration := &conf.NodeConfig{APIHost: "http://panel.invalid", NodeID: 7}
	info := &panel.NodeInfo{Common: &panel.CommonNode{BaseConfig: &panel.BaseConfig{DomainTrafficEnable: true}}}
	old := NewController(nil, configuration, info)
	old.tag = tag
	old.setPendingDomains(&panel.DomainTrafficBatch{ID: "fixed-retry-id", RecordedAt: 123,
		Records: []domaintraffic.Record{{Domain: "retry.example", Upload: 10, Download: 20}}})
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	snapshot := old.RuntimeSnapshot()
	if len(snapshot.UnreportedDomains) != 1 || snapshot.UnreportedDomains[0].Upload != 3 {
		t.Fatalf("unsubmitted counters lost: %+v", snapshot.UnreportedDomains)
	}
	snapshot.UnreportedDomains[0].Upload = 999
	if old.RuntimeSnapshot().UnreportedDomains[0].Upload != 3 {
		t.Fatal("snapshot aliases controller counters")
	}
	snapshot = old.RuntimeSnapshot()
	fresh := NewController(nil, configuration, info)
	fresh.tag = tag
	nodes := &Node{controllers: []*Controller{fresh}}
	nodes.RestorePendingTraffic(RuntimeSnapshot{Nodes: []CachedNode{snapshot}})
	snapshot.UnreportedDomains[0].Upload = 999
	fresh.restoreDomainCounters()
	fresh.restoreDomainCounters()
	counter, release = domaintraffic.Acquire(tag, "fresh.example")
	counter.Add(5, 6)
	release()
	records := domaintraffic.Drain(tag)
	if len(records) != 1 || records[0].Domain != "fresh.example" || records[0].Upload != 8 || records[0].Download != 10 {
		t.Fatalf("restored counters changed or duplicated: %+v", records)
	}
	if len(domaintraffic.Drain(tag)) != 0 || len(fresh.unreportedDomains) != 0 {
		t.Fatal("restored counters retained a second copy")
	}
	batch := fresh.pendingDomains
	if batch == nil || batch.ID != "fixed-retry-id" || batch.RecordedAt != 123 || len(batch.Records) != 1 || batch.Records[0].Upload != 10 {
		t.Fatalf("retry batch mutated: %+v", batch)
	}
}

func TestDomainSnapshotDoesNotRestoreCountersWhenCollectionIsDisabled(t *testing.T) {
	tag := t.Name()
	t.Cleanup(func() { domaintraffic.Remove(tag) })
	controller := NewController(nil, &conf.NodeConfig{}, &panel.NodeInfo{Common: &panel.CommonNode{BaseConfig: &panel.BaseConfig{}}})
	controller.tag = tag
	controller.setUnreportedDomains([]domaintraffic.Record{{Domain: "fresh.example", Upload: 3, Download: 4}})
	controller.restoreDomainCounters()
	if len(controller.unreportedDomains) != 0 {
		t.Fatal("disabled collection retained a backlog")
	}
	domaintraffic.Enable(tag, true)
	if len(domaintraffic.Drain(tag)) != 0 {
		t.Fatal("disabled snapshot reappeared after enable")
	}
}

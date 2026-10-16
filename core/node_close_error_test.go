package core

import (
	"context"
	"errors"
	"testing"

	"github.com/YeJianbo/ravel/common/counter"
	"github.com/YeJianbo/ravel/conf"
	"github.com/YeJianbo/ravel/core/app/dispatcher"
	"github.com/xtls/xray-core/features/inbound"
)

type failingInboundRemoval struct {
	inbound.Manager
	err error
}

func (m *failingInboundRemoval) RemoveHandler(context.Context, string) error { return m.err }

func TestNodeCloseRetainsTrafficWhenInboundRemovalFails(t *testing.T) {
	wantErr := errors.New("inbound already removed")
	c := New(conf.New())
	c.ihm = &failingInboundRemoval{err: wantErr}
	c.dispatcher = &dispatcher.DefaultDispatcher{}
	c.users.uidMap["node|user"] = 7
	counts := counter.NewTrafficCounter()
	counts.Tx("node|user", 31)
	counts.Rx("node|user", 47)
	c.dispatcher.Counter.Store("node", counts)
	traffic, err := c.DelNodeWithTraffic("node")
	if !errors.Is(err, wantErr) {
		t.Fatalf("original removal error lost: %v", err)
	}
	if len(traffic) != 1 || traffic[0].UID != 7 || traffic[0].Upload != 31 || traffic[0].Download != 47 {
		t.Fatalf("remaining traffic lost: %+v", traffic)
	}
	traffic, err = c.DelNodeWithTraffic("node")
	if !errors.Is(err, wantErr) || len(traffic) != 0 {
		t.Fatalf("repeated close duplicated traffic or lost error: %+v, %v", traffic, err)
	}
}

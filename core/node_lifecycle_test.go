package core

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	panel "github.com/YeJianbo/ravel/api/v2board"
	"github.com/YeJianbo/ravel/conf"
	"github.com/YeJianbo/ravel/limiter"
	xnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/proxy/vless/encoding"
)

func TestVlessNodeCloseCapturesTrafficAndReopens(t *testing.T) {
	echo, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = echo.Close() })
	go func() {
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); _, _ = io.Copy(conn, conn) }()
		}
	}()
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := reservation.Addr().(*net.TCPAddr).Port
	_ = reservation.Close()
	info := &panel.NodeInfo{Type: "vless", Tag: "local-close", Common: &panel.CommonNode{
		ListenIP: "127.0.0.1", ServerPort: port, Network: "tcp",
	}}
	// Only this isolated core may dial the loopback echo target; product defaults stay unchanged.
	localOutbound := `{"tag":"loopback-test","protocol":"freedom","settings":{"finalRules":[{"action":"allow","ip":["127.0.0.1"],"network":"tcp"}]}}`
	info.Common.Routes = []panel.Route{{Action: "default_out", ActionValue: &localOutbound}}
	user := panel.UserInfo{Id: 7, Uuid: "3c2c92d0-d032-4c55-834e-9a5c8f57383d"}
	limiter.Init()
	limiter.AddLimiter(info.Type, info.Tag, []panel.UserInfo{user}, map[int]int{})
	t.Cleanup(func() { limiter.DeleteLimiter(info.Tag) })
	c := New(conf.New())
	if err := c.Start([]*panel.NodeInfo{info}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	for _, size := range []int{65536, 17} {
		if err := c.AddNode(info.Tag, info); err != nil {
			t.Fatal(err)
		}
		if _, err := c.AddUsers(&AddUsersParams{Tag: info.Tag, NodeInfo: info, Users: []panel.UserInfo{user}}); err != nil {
			t.Fatal(err)
		}
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", xnet.Port(port).String()), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		memoryUser, err := buildVlessUser(info.Tag, &user, "").ToMemoryUser()
		if err != nil {
			t.Fatal(err)
		}
		request := &protocol.RequestHeader{Version: encoding.Version, Command: protocol.RequestCommandTCP,
			Address: xnet.LocalHostIP, Port: xnet.Port(echo.Addr().(*net.TCPAddr).Port), User: memoryUser}
		if err := encoding.EncodeRequestHeader(conn, request, &encoding.Addons{}); err != nil {
			t.Fatal(err)
		}
		payload := bytes.Repeat([]byte{0x7b}, size)
		if _, err := conn.Write(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := encoding.DecodeResponseHeader(conn, request); err != nil {
			t.Fatal(err)
		}
		reply := make([]byte, size)
		if _, err := io.ReadFull(conn, reply); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(payload, reply) {
			t.Fatal("forwarded bytes changed")
		}
		remaining, err := c.DelNodeWithTraffic(info.Tag)
		if err != nil {
			t.Fatal(err)
		}
		if len(remaining) != 1 || remaining[0].UID != user.Id || remaining[0].Upload != int64(size) || remaining[0].Download != int64(size) {
			t.Fatalf("close counters=%+v, expected %d each", remaining, size)
		}
		if extra, err := c.GetUserTrafficSlice(info.Tag, 0); err != nil || len(extra) != 0 {
			t.Fatalf("second drain=%+v, %v", extra, err)
		}
		_ = conn.Close()
	}
}

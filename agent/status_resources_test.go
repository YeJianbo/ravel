package agent

import (
	"strings"
	"testing"
)

func TestSocketStatisticsKeepEstablishedAndListeningSeparate(t *testing.T) {
	header := "sl local_address rem_address st\n"
	tcp, err := scanSocketTable(strings.NewReader(header+
		"0: 00000000:01BB 00000000:0000 0A\n"+
		"1: 0100007F:D000 0100007F:01BB 01\n"+
		"2: 00000000000000000000000000000000:0016 00000000000000000000000000000000:0000 0A\n"), true)
	if err != nil || tcp.total != 3 || tcp.established != 1 || len(tcp.ports) != 2 || !tcp.ports[443] || !tcp.ports[22] {
		t.Fatalf("TCP snapshot = %+v, error = %v", tcp, err)
	}
	udp, err := scanSocketTable(strings.NewReader(header+"0: 00000000:0035 00000000:0000 07\n"), false)
	if err != nil || udp.total != 1 || udp.established != 0 || !udp.ports[53] {
		t.Fatalf("UDP snapshot = %+v, error = %v", udp, err)
	}
	if got := formatSocketPorts(tcp.ports, udp.ports); got != "TCP 22,443; UDP 53" {
		t.Fatalf("ports = %q", got)
	}
	if _, err := scanSocketTable(strings.NewReader(header+"0: bad missing zz\n"), true); err == nil {
		t.Fatal("malformed table must not become zero connections")
	}
}

func TestEmptySocketTableIsMeasuredZero(t *testing.T) {
	result, err := scanSocketTable(strings.NewReader("sl local_address rem_address st\n"), true)
	if err != nil || result.total != 0 || formatSocketPorts(result.ports, nil) != "" {
		t.Fatalf("empty socket snapshot = %+v, error = %v", result, err)
	}
}

func TestDockerStatisticsRequireExplicitCounts(t *testing.T) {
	values, err := decodeDockerInfo(strings.NewReader(`{"Containers":0,"ContainersRunning":0,"Images":0}`))
	if err != nil || values["docker_total"] != 0 || values["docker_status"] != "available" {
		t.Fatalf("zero Docker snapshot = %#v, error = %v", values, err)
	}
	for _, raw := range []string{`{}`, `{"Containers":1,"ContainersRunning":2,"Images":0}`, `{"Containers":-1,"ContainersRunning":0,"Images":0}`} {
		if _, err := decodeDockerInfo(strings.NewReader(raw)); err == nil {
			t.Fatalf("invalid Docker response accepted: %s", raw)
		}
	}
}

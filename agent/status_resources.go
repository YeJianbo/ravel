package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type socketSnapshot struct {
	total       int
	established int
	ports       map[int]bool
}

var hostVirtualization struct {
	sync.Once
	value string
}

var dockerSnapshot struct {
	sync.Mutex
	captured time.Time
	values   map[string]any
}

func readHostResources(status map[string]any) {
	if runtime.GOOS != "linux" {
		return
	}
	tcp, tcpOK := readSocketTables("tcp")
	udp, udpOK := readSocketTables("udp")
	if tcpOK {
		status["tcp_conn"] = tcp.total
		status["tcp_established"] = tcp.established
	}
	if udpOK {
		status["udp_conn"] = udp.total
	}
	if tcpOK && udpOK {
		status["listen_ports"] = formatSocketPorts(tcp.ports, udp.ports)
	}
	hostVirtualization.Do(func() { hostVirtualization.value = detectHostVirtualization() })
	if hostVirtualization.value != "" {
		status["virtualization"] = hostVirtualization.value
	}
	if value := readTrimmedFile("/proc/sys/net/ipv4/tcp_congestion_control"); value != "" {
		status["tcp_congestion_control"] = value
	}
	for key, value := range readDockerSnapshot() {
		status[key] = value
	}
}

func readSocketTables(protocol string) (socketSnapshot, bool) {
	result := socketSnapshot{ports: map[int]bool{}}
	found := false
	for _, suffix := range []string{"", "6"} {
		file, err := os.Open("/proc/net/" + protocol + suffix)
		if os.IsNotExist(err) {
			continue // IPv6 may be disabled in the current network namespace.
		}
		if err != nil {
			return socketSnapshot{}, false
		}
		part, err := scanSocketTable(file, protocol == "tcp")
		file.Close()
		if err != nil {
			return socketSnapshot{}, false
		}
		found = true
		result.total += part.total
		result.established += part.established
		for port := range part.ports {
			result.ports[port] = true
		}
	}
	return result, found
}

func scanSocketTable(reader io.Reader, tcp bool) (socketSnapshot, error) {
	result := socketSnapshot{ports: map[int]bool{}}
	limited := &io.LimitedReader{R: reader, N: 32 << 20}
	scanner := bufio.NewScanner(limited)
	if !scanner.Scan() || !strings.Contains(scanner.Text(), "local_address") {
		return socketSnapshot{}, fmt.Errorf("missing socket table header")
	}
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 {
			return socketSnapshot{}, fmt.Errorf("invalid socket table row")
		}
		_, portText, ok := strings.Cut(fields[1], ":")
		port, portErr := strconv.ParseUint(portText, 16, 16)
		state, stateErr := strconv.ParseUint(fields[3], 16, 8)
		if !ok || portErr != nil || stateErr != nil {
			return socketSnapshot{}, fmt.Errorf("invalid socket table address or state")
		}
		result.total++
		if tcp && state == 1 {
			result.established++
		}
		// UDP has bound sockets, not TCP-style listening or established connections.
		if port > 0 && ((!tcp) || state == 10) {
			result.ports[int(port)] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return socketSnapshot{}, err
	}
	if limited.N == 0 {
		return socketSnapshot{}, fmt.Errorf("socket table exceeds collection limit")
	}
	return result, nil
}

func formatSocketPorts(tcp, udp map[int]bool) string {
	var parts []string
	for index, ports := range []map[int]bool{tcp, udp} {
		ordered := make([]int, 0, len(ports))
		for port := range ports {
			ordered = append(ordered, port)
		}
		sort.Ints(ordered)
		var values []string
		for _, port := range ordered {
			if len(values) == 128 {
				values = append(values, "...")
				break
			}
			values = append(values, strconv.Itoa(port))
		}
		if len(values) > 0 {
			parts = append(parts, []string{"TCP ", "UDP "}[index]+strings.Join(values, ","))
		}
	}
	return strings.Join(parts, "; ")
}

func detectHostVirtualization() string {
	for _, executable := range []string{"/usr/bin/systemd-detect-virt", "/bin/systemd-detect-virt"} {
		if _, err := os.Stat(executable); err != nil {
			continue
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		output, err := exec.CommandContext(ctx, executable).Output()
		cancel()
		value := strings.TrimSpace(string(output))
		if len(value) <= 64 && value != "" && (err == nil || value == "none") {
			return value
		}
		break
	}
	if _, err := os.Stat("/.dockerenv"); err == nil {
		return "docker"
	}
	if _, err := os.Stat("/run/.containerenv"); err == nil {
		return "container"
	}
	dmi := strings.ToLower(readTrimmedFile("/sys/class/dmi/id/sys_vendor") + " " + readTrimmedFile("/sys/class/dmi/id/product_name"))
	for _, entry := range [][2]string{{"kvm", "kvm"}, {"qemu", "qemu"}, {"vmware", "vmware"}, {"virtualbox", "oracle"}, {"xen", "xen"}, {"microsoft corporation virtual machine", "microsoft"}} {
		if strings.Contains(dmi, entry[0]) {
			return entry[1]
		}
	}
	return ""
}

func readDockerSnapshot() map[string]any {
	dockerSnapshot.Lock()
	defer dockerSnapshot.Unlock()
	if dockerSnapshot.values != nil && time.Since(dockerSnapshot.captured) < time.Minute {
		return dockerSnapshot.values
	}
	values := map[string]any{"docker_status": "unavailable", "docker_summary": "Docker statistics unavailable"}
	const socket = "/var/run/docker.sock"
	if _, err := os.Stat(socket); os.IsNotExist(err) {
		if _, binaryErr := exec.LookPath("docker"); binaryErr != nil {
			values = map[string]any{"docker_status": "not_installed", "docker_summary": "Docker not detected"}
		}
	} else if err == nil {
		// Fixed local GET only: never accept daemon addresses, paths, or commands from the panel.
		transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", socket)
		}}
		client := &http.Client{Transport: transport, Timeout: time.Second, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
		response, err := client.Get("http://docker/info")
		if err == nil {
			if response.StatusCode == http.StatusOK {
				if stats, decodeErr := decodeDockerInfo(response.Body); decodeErr == nil {
					values = stats
				}
			}
			response.Body.Close()
		}
		transport.CloseIdleConnections()
	}
	dockerSnapshot.values = values
	dockerSnapshot.captured = time.Now()
	return values
}

func decodeDockerInfo(reader io.Reader) (map[string]any, error) {
	var info struct {
		Containers        *int `json:"Containers"`
		ContainersRunning *int `json:"ContainersRunning"`
		Images            *int `json:"Images"`
	}
	limited := &io.LimitedReader{R: reader, N: 1 << 20}
	decoder := json.NewDecoder(limited)
	if err := decoder.Decode(&info); err != nil {
		return nil, err
	}
	if info.Containers == nil || info.ContainersRunning == nil || info.Images == nil || *info.Containers < 0 || *info.ContainersRunning < 0 || *info.Images < 0 || *info.ContainersRunning > *info.Containers {
		return nil, fmt.Errorf("invalid Docker statistics")
	}
	return map[string]any{
		"docker_status": "available", "docker_total": *info.Containers,
		"docker_running": *info.ContainersRunning, "docker_images": *info.Images,
		"docker_summary": fmt.Sprintf("Running %d/%d; images %d", *info.ContainersRunning, *info.Containers, *info.Images),
	}, nil
}

package backend

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

func boolPtr(b bool) *bool { return &b }

func TestBackendDialer_KeepAliveEnabled(t *testing.T) {
	d := backendDialer(7*time.Second, config.KeepAliveConfig{
		Enabled:  boolPtr(true),
		Idle:     30 * time.Second,
		Interval: 10 * time.Second,
		Count:    3,
	})
	if d.Timeout != 7*time.Second {
		t.Errorf("expected dial timeout 7s, got %v", d.Timeout)
	}
	if !d.KeepAliveConfig.Enable {
		t.Fatal("expected KeepAliveConfig.Enable true")
	}
	if d.KeepAliveConfig.Idle != 30*time.Second ||
		d.KeepAliveConfig.Interval != 10*time.Second ||
		d.KeepAliveConfig.Count != 3 {
		t.Errorf("unexpected KeepAliveConfig: %+v", d.KeepAliveConfig)
	}
}

func TestBackendDialer_KeepAliveDisabled(t *testing.T) {
	// nil Enabled (e.g. the shadow target, never run through config.Load)
	// must leave keep-alive off — KeepAlive=-1 disables Go's default too.
	d := backendDialer(3*time.Second, config.KeepAliveConfig{})
	if d.KeepAliveConfig.Enable {
		t.Error("expected KeepAliveConfig.Enable false for nil Enabled")
	}
	if d.KeepAlive != -1 {
		t.Errorf("expected KeepAlive=-1 (disabled), got %v", d.KeepAlive)
	}

	// Explicit enabled:false behaves the same.
	d = backendDialer(3*time.Second, config.KeepAliveConfig{Enabled: boolPtr(false)})
	if d.KeepAliveConfig.Enable || d.KeepAlive != -1 {
		t.Errorf("expected keep-alive disabled for enabled:false, got Enable=%v KeepAlive=%v",
			d.KeepAliveConfig.Enable, d.KeepAlive)
	}
}

// TestBackendDialer_DialRespectsTimeout verifies the dialer actually
// enforces the connect timeout (go-mysql's ConnectWithTimeout did not —
// the reason we dial ourselves). Dialing an unroutable address must fail
// within roughly the configured timeout, not hang.
func TestBackendDialer_DialRespectsTimeout(t *testing.T) {
	d := backendDialer(150*time.Millisecond, config.KeepAliveConfig{Enabled: boolPtr(true), Idle: time.Second, Interval: time.Second, Count: 1})
	// 192.0.2.0/24 (TEST-NET-1) is reserved and unroutable → connect blocks
	// until the dial timeout fires.
	start := time.Now()
	_, err := d.Dial("tcp", "192.0.2.1:3306")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected dial to fail to an unroutable address")
	}
	if elapsed > time.Second {
		t.Fatalf("dial took %v; timeout (150ms) was not honored", elapsed)
	}
}

func TestCollationName(t *testing.T) {
	cases := []struct {
		id   uint8
		want string
	}{
		{45, "utf8mb4_general_ci"},
		{255, "utf8mb4_0900_ai_ci"},
		{33, "utf8_general_ci"},
		{0, ""},
	}
	for _, c := range cases {
		if got := CollationName(c.id); got != c.want {
			t.Errorf("CollationName(%d) = %q, want %q", c.id, got, c.want)
		}
	}
}

// greeting builds a HandshakeV10 payload with the given version and charset.
func greeting(version string, charsetID byte) []byte {
	p := []byte{10}
	p = append(p, version...)
	p = append(p, 0)
	p = append(p, 1, 0, 0, 0)             // connection id
	p = append(p, 1, 2, 3, 4, 5, 6, 7, 8) // auth-plugin-data-part-1
	p = append(p, 0)                      // filler
	p = append(p, 0xff, 0xf7)             // capability lower
	p = append(p, charsetID)
	p = append(p, 2, 0) // status
	return p
}

func frame(payload []byte) []byte {
	return append([]byte{byte(len(payload)), byte(len(payload) >> 8), byte(len(payload) >> 16), 0}, payload...)
}

func TestReadGreetingCollation(t *testing.T) {
	got, err := readGreetingCollation(bytes.NewReader(frame(greeting("8.0.43", 45))))
	if err != nil || got != 45 {
		t.Fatalf("got (%d, %v), want (45, nil)", got, err)
	}

	errPkt := frame(append([]byte{0xff, 0x10, 0x04, '#', 'H', 'Y', '0', '0', '0'}, "Too many connections"...))
	if _, err := readGreetingCollation(bytes.NewReader(errPkt)); err == nil {
		t.Error("expected an error for an ERR packet")
	}
	if _, err := readGreetingCollation(bytes.NewReader(frame(greeting("8.0.43", 45)[:12]))); err == nil {
		t.Error("expected an error for a truncated greeting")
	}
	if _, err := readGreetingCollation(bytes.NewReader(nil)); err == nil {
		t.Error("expected an error for an empty stream")
	}
}

func TestProbeCollation(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Write(frame(greeting("8.0.43", 45)))
	}()

	got, err := ProbeCollation(config.BackendConfig{Addr: ln.Addr().String()}, 2*time.Second)
	if err != nil || got != 45 {
		t.Fatalf("got (%d, %v), want (45, nil)", got, err)
	}

	ln.Close()
	if _, err := ProbeCollation(config.BackendConfig{Addr: ln.Addr().String()}, time.Second); err == nil {
		t.Error("expected an error when the backend is unreachable")
	}
}

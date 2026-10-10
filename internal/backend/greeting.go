package backend

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/takaidohigasi/mysql-interceptor/internal/config"
)

// ProbeCollation connects to the backend, reads the initial handshake
// packet the server sends before authentication, and returns the server's
// default collation id from it. No credentials are needed and nothing is
// sent: the connection is closed right after the greeting is read.
//
// The proxy advertises this id to its own clients. Clients that do not pick
// a charset themselves (e.g. PHP's mysqlnd) echo the greeting's collation
// back in their handshake, so advertising the backend's default makes them
// behave as if they had connected to the backend directly.
func ProbeCollation(cfg config.BackendConfig, timeout time.Duration) (uint8, error) {
	network := "tcp"
	if strings.Contains(cfg.Addr, "/") {
		network = "unix"
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := backendDialer(timeout, cfg.KeepAlive).DialContext(ctx, network, cfg.Addr)
	if err != nil {
		return 0, fmt.Errorf("probing backend %s: %w", cfg.Addr, err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))

	id, err := readGreetingCollation(conn)
	if err != nil {
		return 0, fmt.Errorf("probing backend %s: %w", cfg.Addr, err)
	}
	return id, nil
}

// readGreetingCollation parses the server's Protocol::HandshakeV10 packet
// and returns its character_set byte.
func readGreetingCollation(r io.Reader) (uint8, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, fmt.Errorf("reading handshake header: %w", err)
	}
	n := int(binary.LittleEndian.Uint32(append(hdr[:3:3], 0)))
	if n == 0 || n > 64*1024 {
		return 0, fmt.Errorf("unexpected handshake packet length %d", n)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, fmt.Errorf("reading handshake payload: %w", err)
	}
	if payload[0] == 0xff {
		// ERR packet instead of a greeting (e.g. too many connections).
		msg := ""
		if len(payload) > 9 {
			msg = string(payload[9:])
		}
		return 0, fmt.Errorf("backend returned an error instead of a handshake: %s", msg)
	}
	if payload[0] != 10 {
		return 0, fmt.Errorf("unsupported handshake protocol version %d", payload[0])
	}
	// protocol(1) | server version (NUL-terminated) | conn id(4) |
	// auth-plugin-data-part-1(8) | filler(1) | capability lower(2) | charset(1)
	end := bytes.IndexByte(payload[1:], 0)
	if end < 0 {
		return 0, fmt.Errorf("malformed handshake: unterminated server version")
	}
	pos := 1 + end + 1 + 4 + 8 + 1 + 2
	if len(payload) <= pos {
		return 0, fmt.Errorf("malformed handshake: packet too short")
	}
	return payload[pos], nil
}

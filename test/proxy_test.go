package test

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/takaidohigasi/mysql-interceptor/internal/config"
	"github.com/takaidohigasi/mysql-interceptor/internal/proxy"
)

// startInProcessProxy spins up a ProxyServer listening on a free port and
// forwarding to the configured MYSQL1_ADDR, accepting root/rootpass. Returns
// the proxy's listen address and a cleanup func. Skips if no MySQL is
// reachable.
func startInProcessProxy(t *testing.T) (string, func()) {
	t.Helper()
	return startInProcessProxyWithUsers(t, []config.UserConfig{
		{Username: "root", Password: "rootpass"},
	})
}

// startInProcessProxyWithUsers is startInProcessProxy with an explicit
// proxy.users list. Optional mutators adjust the config before the proxy
// is created.
func startInProcessProxyWithUsers(t *testing.T, users []config.UserConfig, mutate ...func(*config.Config)) (string, func()) {
	t.Helper()
	skipIfNoMySQL(t)

	// Pick a free port for the proxy listener.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("picking proxy port: %v", err)
	}
	addr := l.Addr().String()
	l.Close()

	backendAddr := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")

	cfg := &config.Config{
		Proxy: config.ProxyConfig{
			ListenAddr:      addr,
			ShutdownTimeout: 5 * time.Second,
			Users:           users,
		},
		Backend: config.BackendConfig{
			Addr: backendAddr,
			DB:   "test_db",
		},
	}

	for _, m := range mutate {
		m(cfg)
	}

	srv, err := proxy.NewProxyServer(cfg, nil, nil)
	if err != nil {
		t.Fatalf("creating proxy: %v", err)
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- srv.Serve()
	}()

	// Wait for the proxy to accept TCP connections.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			c.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	cleanup := func() {
		srv.Shutdown()
		select {
		case <-serveDone:
		case <-time.After(10 * time.Second):
			t.Errorf("proxy did not shut down within 10s")
		}
	}
	return addr, cleanup
}

// TestProxyPreparedStatementRoundTrip connects through the proxy using the
// standard database/sql driver, which uses the binary (prepared statement)
// protocol by default. This exercises HandleStmtPrepare/Execute/Close.
func TestProxyPreparedStatementRoundTrip(t *testing.T) {
	addr, cleanup := startInProcessProxy(t)
	defer cleanup()

	dsn := fmt.Sprintf("root:rootpass@tcp(%s)/test_db?timeout=10s", addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := db.Ping(); err != nil {
		t.Fatalf("ping via proxy: %v", err)
	}

	// database/sql automatically prepares this query on the backend.
	var name string
	err = db.QueryRow("SELECT name FROM users WHERE id = ?", 1).Scan(&name)
	if err != nil {
		t.Fatalf("prepared SELECT via proxy: %v", err)
	}
	if name != "alice" {
		t.Errorf("expected name=alice, got %q", name)
	}

	// Run the same prepared stmt with a different binding — confirms the
	// Stmt is reusable across executions.
	rows, err := db.Query("SELECT id, name FROM users WHERE id > ? ORDER BY id", 1)
	if err != nil {
		t.Fatalf("prepared ranged SELECT: %v", err)
	}
	defer rows.Close()

	var got []string
	for rows.Next() {
		var id int
		var n string
		if err := rows.Scan(&id, &n); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got = append(got, n)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 rows (bob, charlie), got %d: %v", len(got), got)
	}
}

// TestProxyGracefulShutdown verifies the proxy drains cleanly under
// Shutdown() even with an idle client connection held open.
func TestProxyGracefulShutdown(t *testing.T) {
	addr, cleanup := startInProcessProxy(t)

	dsn := fmt.Sprintf("root:rootpass@tcp(%s)/test_db?timeout=10s", addr)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	// Establish a session so the proxy has an active connection to drain.
	if _, err := db.Exec("SELECT 1"); err != nil {
		t.Fatalf("warmup exec: %v", err)
	}

	// cleanup invokes Shutdown; the test fails if it exceeds the deadline.
	start := time.Now()
	cleanup()
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Errorf("shutdown took too long: %v", elapsed)
	}
}

// TestProxyHashedPasswordUserE2E is the ProxySQL-style flow against a real
// MySQL: the proxy is configured with only the mysql_native_password hash of
// hashed_user, the client logs in with the plaintext, and the proxy opens
// the backend connection with the SHA1 stage1 value recovered from the
// handshake. Queries must flow; a wrong plaintext must be rejected.
func TestProxyHashedPasswordUserE2E(t *testing.T) {
	skipIfNoMySQL(t)

	const (
		user = "hashed_user"
		pw   = "hashed-pw-1"
	)
	backendAddr := getEnvOrDefault("MYSQL1_ADDR", "127.0.0.1:3306")

	// Provision a native-password account on the backend. MySQL 8.0 still
	// ships the plugin enabled; the CI image is mysql:8.0.
	root, err := client.Connect(backendAddr, "root", "rootpass", "test_db")
	if err != nil {
		t.Fatalf("root connect: %v", err)
	}
	defer root.Close()
	for _, q := range []string{
		fmt.Sprintf("CREATE USER IF NOT EXISTS '%s'@'%%' IDENTIFIED WITH mysql_native_password BY '%s'", user, pw),
		fmt.Sprintf("GRANT SELECT ON test_db.* TO '%s'@'%%'", user),
	} {
		if _, err := root.Execute(q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	t.Cleanup(func() {
		_, _ = root.Execute(fmt.Sprintf("DROP USER IF EXISTS '%s'@'%%'", user))
	})

	// The proxy sees only the stored hash, exactly what
	// mysql.user.authentication_string holds for this account.
	hashed := mysql.EncodePasswordHex(mysql.NativePasswordHash([]byte(pw)))
	proxyAddr, cleanup := startInProcessProxyWithUsers(t, []config.UserConfig{
		{Username: user, HashedPassword: hashed},
	})
	defer cleanup()

	// Correct plaintext: handshake, backend connect via stage1, and a
	// query through the proxy all succeed.
	conn, err := client.Connect(proxyAddr, user, pw, "test_db")
	if err != nil {
		t.Fatalf("connect through proxy as hashed user: %v", err)
	}
	defer conn.Close()
	res, err := conn.Execute("SELECT id FROM users ORDER BY id")
	if err != nil {
		t.Fatalf("query through proxy: %v", err)
	}
	if got := len(res.Values); got != 3 {
		t.Errorf("expected 3 rows from users, got %d", got)
	}
	who, err := conn.Execute("SELECT CURRENT_USER()")
	if err != nil {
		t.Fatalf("SELECT CURRENT_USER(): %v", err)
	}
	if cu, _ := who.GetString(0, 0); cu != user+"@%" {
		t.Errorf("backend session user = %q, want %q", cu, user+"@%")
	}

	// Wrong plaintext: rejected at the proxy handshake.
	if bad, err := client.Connect(proxyAddr, user, "not-the-password", "test_db"); err == nil {
		bad.Close()
		t.Error("expected access denied for wrong password, got nil")
	}
}

// TestProxyPropagatesClientCollation verifies the backend session comes up
// with the collation the client announced in its handshake, rather than
// go-mysql's utf8mb4_0900_ai_ci default.
func TestProxyPropagatesClientCollation(t *testing.T) {
	addr, cleanup := startInProcessProxy(t)
	defer cleanup()

	for _, collation := range []string{"utf8mb4_general_ci", "utf8mb4_0900_ai_ci", "utf8mb4_unicode_ci"} {
		t.Run(collation, func(t *testing.T) {
			dsn := fmt.Sprintf("root:rootpass@tcp(%s)/test_db?timeout=10s&collation=%s", addr, collation)
			db, err := sql.Open("mysql", dsn)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)

			var charset, coll string
			if err := db.QueryRow("SELECT @@character_set_connection, @@collation_connection").Scan(&charset, &coll); err != nil {
				t.Fatalf("query via proxy: %v", err)
			}
			if charset != "utf8mb4" || coll != collation {
				t.Errorf("backend session got %s/%s, want utf8mb4/%s", charset, coll, collation)
			}
		})
	}
}

// Collation ids as the MySQL protocol carries them: one byte in the
// handshake. The id <-> name mapping is listed in
// information_schema.COLLATIONS (the ID column), e.g.
//
//	SELECT ID, COLLATION_NAME FROM information_schema.COLLATIONS
//	WHERE COLLATION_NAME IN ('utf8mb4_general_ci', 'utf8mb4_0900_ai_ci');
//
// See https://dev.mysql.com/doc/refman/8.0/en/information-schema-collations-table.html
const (
	collationIDUTF8MB4GeneralCI = 45  // utf8mb4_general_ci
	collationIDUTF8MB40900AICI  = 255 // utf8mb4_0900_ai_ci (go-mysql's and MySQL 8.0's default)
)

// greetingCollationID dials addr and returns the collation id from the
// server's initial handshake packet. The server sends this packet first,
// before the client authenticates, so no credentials are needed.
//
// Packet layout (Protocol::HandshakeV10), after the 4-byte packet header
// (3-byte little-endian payload length + 1-byte sequence id):
//
//	1 byte    protocol version (10)
//	NUL-term  server version string
//	4 bytes   connection id
//	8 bytes   auth-plugin-data-part-1
//	1 byte    filler (0x00)
//	2 bytes   capability flags (lower 2 bytes)
//	1 byte    character set  <- the collation id this function returns
//	...
//
// See https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_connection_phase_packets_protocol_handshake_v10.html
func greetingCollationID(t *testing.T, addr string) uint8 {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer c.Close()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	// Packet header: 3-byte little-endian payload length, then the sequence id.
	var hdr [4]byte
	if _, err := io.ReadFull(c, hdr[:]); err != nil {
		t.Fatalf("reading greeting header: %v", err)
	}
	payload := make([]byte, int(hdr[0])|int(hdr[1])<<8|int(hdr[2])<<16)
	if _, err := io.ReadFull(c, payload); err != nil {
		t.Fatalf("reading greeting: %v", err)
	}
	// Skip protocol version (1), the NUL-terminated server version, connection
	// id (4), auth-plugin-data-part-1 (8), filler (1) and capability flags (2).
	versionLen := bytes.IndexByte(payload[1:], 0)
	charsetPos := 1 + versionLen + 1 + 4 + 8 + 1 + 2
	return payload[charsetPos]
}

// TestProxyDefaultCollationInGreeting verifies proxy.default_collation is
// what the proxy advertises in its greeting, and that a client echoing it
// (as PHP's mysqlnd does when it picks no charset) gets a backend session
// with that collation.
func TestProxyDefaultCollationInGreeting(t *testing.T) {
	addr, cleanup := startInProcessProxyWithUsers(t,
		[]config.UserConfig{{Username: "root", Password: "rootpass"}},
		func(c *config.Config) { c.Proxy.DefaultCollation = "utf8mb4_general_ci" })
	defer cleanup()

	if got := greetingCollationID(t, addr); got != collationIDUTF8MB4GeneralCI {
		t.Errorf("greeting collation id = %d, want %d (utf8mb4_general_ci)", got, collationIDUTF8MB4GeneralCI)
	}
}

// TestProxyDefaultCollationUnset keeps the historical greeting
// (utf8mb4_0900_ai_ci, id 255) when proxy.default_collation is empty.
func TestProxyDefaultCollationUnset(t *testing.T) {
	addr, cleanup := startInProcessProxy(t)
	defer cleanup()

	if got := greetingCollationID(t, addr); got != collationIDUTF8MB40900AICI {
		t.Errorf("greeting collation id = %d, want %d (utf8mb4_0900_ai_ci)", got, collationIDUTF8MB40900AICI)
	}
}

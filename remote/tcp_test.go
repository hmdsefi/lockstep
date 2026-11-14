// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package remote_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// testTLS returns a TLS configuration with a self-signed certificate for 127.0.0.1 that also
// trusts it, for both ends of a connection.
func testTLS(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "lockstep test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		RootCAs:      pool,
		MinVersion:   tls.VersionTLS13,
	}
}

// TestTCPOverTLS checks that two systems with TLS configured exchange an ask over TLS, the
// dialer verifying the listener's certificate against the dialed host.
func TestTCPOverTLS(t *testing.T) {
	cfg := testTLS(t)
	reg := newRegistry()
	b := startNode(t, "b", reg, func(c *remote.Config) { c.TLS = cfg })
	a := startNode(t, "a", reg, func(c *remote.Config) {
		c.TLS = cfg
		c.Peers = map[string]string{"b": b.addr}
	})
	spawn[Echo](t, b.sys, "echo", echo{tag: "b"})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if v, err := ask(a.sys, "hi", 10*time.Second).Await(ctx); err != nil || v != "hi from b" {
		t.Errorf("Ask over TLS = %q, %v; want %q", v, err, "hi from b")
	}
}

// TestTCPProtocolErrors checks that a length prefix out of range, from a peer that completed its
// handshake, is answered with CLOSE protocol and ends the connection, and that the system keeps
// serving other connections.
func TestTCPProtocolErrors(t *testing.T) {
	reg := newRegistry()
	b := startNode(t, "b", reg, nil)
	spawn[Echo](t, b.sys, "echo", echo{tag: "b"})

	conn, err := net.Dial("tcp", b.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writeFrame(t, conn, helloFrame(remoting.KindHello, "r", "b", ""))
	mustRead(t, conn, remoting.KindWelcome)
	if _, err := conn.Write([]byte{0xff, 0xff, 0xff, 0xff, 1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if f := mustRead(t, conn, remoting.KindClose); string(f.Payload) != remoting.CloseProtocol {
		t.Errorf("CLOSE %q, want %q", f.Payload, remoting.CloseProtocol)
	}
	if _, err := readFrame(t, conn); !errors.Is(err, io.EOF) {
		t.Errorf("after CLOSE read %v, want EOF", err)
	}

	a := startNode(t, "a", reg, func(c *remote.Config) { c.Peers = map[string]string{"b": b.addr} })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if v, err := ask(a.sys, "hi", 10*time.Second).Await(ctx); err != nil || v != "hi from b" {
		t.Errorf("Ask after the protocol error = %q, %v", v, err)
	}
}

// TestTCPTLSSilentPeerShutdown checks that Shutdown of a node listening with TLS returns promptly,
// with every goroutine gone, while peers hold connections whose TLS handshake never completes:
// one that sends nothing and one that stops halfway through its ClientHello. The dial timeout is
// far longer than the bound, so the shutdown itself must end the handshakes.
func TestTCPTLSSilentPeerShutdown(t *testing.T) {
	leaks := goleak.IgnoreCurrent()
	n := newNode(t, "a")
	n.enable(t, remote.Config{Listen: "127.0.0.1:0", Registry: newRegistry(), TLS: testTLS(t), DialTimeout: time.Minute})

	silent, err := net.Dial("tcp", n.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()
	partial, err := net.Dial("tcp", n.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer partial.Close()
	// A TLS record header announcing a 512-byte handshake record, and only part of it.
	if _, err := partial.Write([]byte{0x16, 0x03, 0x01, 0x02, 0x00, 0x01, 0x00, 0x01, 0xfc}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // let the node accept both and start their handshakes

	done := make(chan error, 1)
	start := time.Now()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		done <- n.sys.Shutdown(ctx)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return within 10 s")
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Errorf("Shutdown took %v, want at most 5 s", d)
	}
	goleak.VerifyNone(t, leaks)
}

// TestTCPWildcardAdvertise checks that a node listening on every interface, with no Advertise,
// announces no address in its HELLO, while one listening on a specific host announces it.
func TestTCPWildcardAdvertise(t *testing.T) {
	for _, tc := range []struct {
		name, listen string
		wildcard     bool
	}{{"Wildcard", "0.0.0.0:0", true}, {"Loopback", "127.0.0.1:0", false}} {
		t.Run(tc.name, func(t *testing.T) {
			peer := rawListener(t)
			n := startNode(t, "a", newRegistry(), func(c *remote.Config) {
				c.Listen = tc.listen
				c.Peers = map[string]string{"b": peer.Addr().String()}
			})
			if err := lockstep.Send(n.sys, lockstep.PIDAt[Note]("b", "/user/x"), Note{}); err != nil {
				t.Fatal(err)
			}
			conn := acceptWithin(t, peer, 10*time.Second)
			if conn == nil {
				t.Fatal("a did not dial b")
			}
			h, err := remoting.ParseHello(mustRead(t, conn, remoting.KindHello).Payload)
			if err != nil {
				t.Fatal(err)
			}
			if want := map[bool]string{true: "", false: n.addr}[tc.wildcard]; h.Advertise != want {
				t.Errorf("HELLO announces %q, want %q", h.Advertise, want)
			}
		})
	}
}

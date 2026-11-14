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

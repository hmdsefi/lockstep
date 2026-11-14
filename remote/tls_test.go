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
	"math/big"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/hmdsefi/lockstep"
	"github.com/hmdsefi/lockstep/internal/remoting"
	"github.com/hmdsefi/lockstep/remote"
)

// testCA is a certificate authority made inside a test.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

// newCA returns a new self-signed certificate authority named name.
func newCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
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
	return &testCA{cert: cert, key: key, pool: pool}
}

// leaf returns a certificate for 127.0.0.1 named name and signed by ca, good for servers and
// clients.
func (ca *testCA) leaf(t *testing.T, name string, serial int64) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}

// config returns the TLS configuration of a node that presents cert and trusts ca, both as a
// server, which requires and verifies the client's certificate, and as a client.
func (ca *testCA) config(cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      ca.pool,
		ClientCAs:    ca.pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}
}

// TestATREM24TLS checks REM-380 with a certificate authority and two leaf certificates made in the
// test: two nodes that trust the authority exchange an ask over mutual TLS; a peer that speaks
// plain TCP, and one whose certificate another authority signed, fail the handshake, and the
// session goes down with reason io (the core's "link closed" log, which only that path writes),
// whichever side dialed. A dialer whose session went down that way dials again.
func TestATREM24TLS(t *testing.T) {
	ca, other := newCA(t, "lockstep test CA"), newCA(t, "another CA")
	reg := newRegistry()
	b := startNode(t, "b", reg, func(c *remote.Config) { c.TLS = ca.config(ca.leaf(t, "b", 2)) })
	ln := rawListener(t)
	a := startNode(t, "a", reg, func(c *remote.Config) {
		c.TLS = ca.config(ca.leaf(t, "a", 3))
		c.Peers = map[string]string{"b": b.addr, "r": ln.Addr().String()}
	})
	spawn[Echo](t, b.sys, "echo", echo{tag: "b"})

	t.Run("trusted", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), rawWait)
		defer cancel()
		if v, err := ask(a.sys, "hi", rawWait).Await(ctx); err != nil || v != "hi from b" {
			t.Errorf("Ask over TLS = %q, %v; want %q", v, err, "hi from b")
		}
	})

	t.Run("plaintext dialer", func(t *testing.T) {
		before := closedLinks(b, "")
		r := dialRaw(t, b.addr)
		r.write(*helloFrame(remoting.KindHello, "r", "b", ""))
		if got, err := readRaw(t, r.conn); err == nil {
			t.Fatalf("plain TCP peer read % x, want the connection closed", got)
		}
		waitClosed(t, b, "", before, "tls")
	})

	t.Run("wrong-CA dialer", func(t *testing.T) {
		before := closedLinks(b, "")
		conn, err := net.Dial("tcp", b.addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		cfg := other.config(other.leaf(t, "r", 4))
		cfg.RootCAs, cfg.ServerName = ca.pool, "127.0.0.1" // r trusts b, but b does not trust r
		tc := tls.Client(conn, cfg)
		if err := tc.Handshake(); err == nil {
			// A TLS 1.3 client finishes before the server checks its certificate; the server's
			// refusal arrives with the next read, or fails the write when it came first. Either is
			// the refusal.
			_, err := tc.Write(remoting.AppendFrame(nil, helloFrame(remoting.KindHello, "r", "b", "")))
			if err == nil {
				if got, err := readRaw(t, tc); err == nil {
					t.Fatalf("b answered a peer whose certificate another authority signed: % x", got)
				}
			}
		}
		waitClosed(t, b, "", before, "certificate")
	})

	t.Run("plaintext acceptor", func(t *testing.T) {
		before := closedLinks(a, "r")
		sendNote(t, a.sys, "r")
		r := acceptRaw(t, ln)
		// a's ClientHello is no frame; r answers it with a plain WELCOME.
		r.write(*helloFrame(remoting.KindWelcome, "r", "a", ""))
		waitClosed(t, a, "r", before, "tls")
		acceptRaw(t, ln) // a dials again
	})

	t.Run("wrong-CA acceptor", func(t *testing.T) {
		before := closedLinks(a, "r")
		sendNote(t, a.sys, "r")
		conn := acceptWithin(t, ln, rawWait)
		if conn == nil {
			t.Fatal("a did not dial r")
		}
		srv := tls.Server(conn, other.config(other.leaf(t, "r", 5)))
		if err := srv.Handshake(); err == nil {
			t.Error("a finished a handshake with a server whose certificate another authority signed")
		}
		waitClosed(t, a, "r", before, "certificate")
		acceptRaw(t, ln) // a dials again
	})
}

// closedLinks returns the number of "remoting: link closed" records n logged for peer.
func closedLinks(n *node, peer string) int {
	return len(n.log.find("remoting: link closed", map[string]string{"node": n.sys.Node(), "peer": peer}))
}

// waitClosed waits until n logs a "remoting: link closed" record for peer, after the first before
// of them, whose error names want. The core logs it only when a link ends with an error, and then
// takes the session down with reason io.
func waitClosed(t *testing.T, n *node, peer string, before int, want string) {
	t.Helper()
	deadline := time.Now().Add(rawWait)
	for {
		recs := n.log.find("remoting: link closed", map[string]string{"node": n.sys.Node(), "peer": peer})
		for _, r := range recs[min(before, len(recs)):] {
			if strings.Contains(r.attrs["err"], want) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s logged no link closed for peer %q with an error naming %q: %v", n.sys.Node(), peer, want, recs[min(before, len(recs)):])
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// sendNote sends a Note from sys to /user/x on node.
func sendNote(t *testing.T, sys *lockstep.System, node string) {
	t.Helper()
	if err := lockstep.Send(sys, lockstep.PIDAt[Note](node, "/user/x"), Note{}); err != nil {
		t.Fatal(err)
	}
}

package grinta

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"log/slog"
	"math/big"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-metrics"
	"github.com/stretchr/testify/require"
)

func generateKeyPair(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("failed to generate private key: %s", err)
		return nil
	}
	return key
}

func generateCa(t *testing.T, pkey *ecdsa.PrivateKey) []byte {
	t.Helper()
	notBefore := time.Now()
	notAfter := time.Now().Add(1 * time.Hour)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("failed to generate serialNumber: %s", err)
	}
	tmpl := x509.Certificate{
		Subject: pkix.Name{
			CommonName: "self-signed",
		},
		SerialNumber:          serialNumber,
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IPAddresses: []net.IP{
			{127, 0, 0, 1},
		},
		IsCA: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &pkey.PublicKey, pkey)
	if err != nil {
		t.Fatalf("failed to generate CA: %s", err)
		return nil
	}
	return certDER
}

func generateLeaf(t *testing.T, ca *x509.Certificate, caKP, leafKP *ecdsa.PrivateKey, cn string) []byte {
	t.Helper()
	notBefore := time.Now()
	notAfter := time.Now().Add(1 * time.Hour)

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("failed to generate serialNumber: %s", err)
	}
	tmpl := x509.Certificate{
		Subject: pkix.Name{
			CommonName: cn,
		},
		SerialNumber: serialNumber,
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		IPAddresses: []net.IP{
			{127, 0, 0, 1},
		},
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IsCA:                  false,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, &tmpl, ca, &leafKP.PublicKey, caKP)
	if err != nil {
		t.Fatalf("failed to generate CA: %s", err)
		return nil
	}
	return certDER
}

func TestNewTransport(t *testing.T) {
	n1handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}).WithAttrs([]slog.Attr{
		{Key: "emitter", Value: slog.StringValue("node1")},
	})

	n2handler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level:     slog.LevelDebug,
		AddSource: true,
	}).WithAttrs([]slog.Attr{
		{Key: "emitter", Value: slog.StringValue("node2")},
	})

	caKey := generateKeyPair(t)
	node1Key := generateKeyPair(t)
	node2Key := generateKeyPair(t)

	caDER := generateCa(t, caKey)
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("failed to parse CA: %s", err)
		return
	}

	node1DER := generateLeaf(t, ca, caKey, node1Key, "node1")
	node1, err := x509.ParseCertificate(node1DER)
	if err != nil {
		t.Fatalf("failed to parse node1: %s", err)
		return
	}

	node2DER := generateLeaf(t, ca, caKey, node2Key, "node2")
	node2, err := x509.ParseCertificate(node2DER)
	if err != nil {
		t.Fatalf("failed to parse node2: %s", err)
		return
	}

	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	tcN1 := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{node1DER},
				Leaf:        node1,
				PrivateKey:  node1Key,
			},
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  caPool,
		RootCAs:    caPool,
	}

	tcN2 := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{node2DER},
				Leaf:        node2,
				PrivateKey:  node2Key,
			},
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  caPool,
		RootCAs:    caPool,
	}

	node1Metrics := metrics.NewInmemSink(time.Second, 5*time.Minute)
	node2Metrics := metrics.NewInmemSink(time.Second, 5*time.Minute)

	ts1, err := NewTransport(&TransportConfig{
		TlsConfig:  tcN1,
		BindAddr:   "127.0.0.1",
		BindPort:   6021,
		MetricSink: node1Metrics,
		LogHandler: n1handler,
	})
	if err != nil {
		t.Fatalf("failed to start node1: %s", err)
		return
	}

	ts2, err := NewTransport(&TransportConfig{
		TlsConfig:  tcN2,
		BindAddr:   "127.0.0.1",
		BindPort:   6022,
		MetricSink: node2Metrics,
		LogHandler: n2handler,
	})
	if err != nil {
		t.Fatalf("failed to start node2: %s", err)
		return
	}

	t.Run("write datagram from n1 to n2", func(t *testing.T) {
		_, err = ts1.WriteTo([]byte("hello"), "localhost:6022")
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		select {
		case packet := <-ts2.PacketCh():
			t.Logf("received %s from peer %s", packet.Buf, packet.From)
			require.Equal(t, "hello", string(packet.Buf), "unexpected packet data")
			cancel()
		case <-ctx.Done():
			t.Fatalf("timed out")
		}
	})

	t.Run("open stream from n2 to n1", func(t *testing.T) {
		ts := time.Now()
		conn, err := ts2.DialTimeout("localhost:6021", 1*time.Minute)
		ts2 := time.Now()
		require.NoError(t, err)
		t.Logf("dialing took %s", ts2.Sub(ts).String())

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		select {
		case stream := <-ts1.StreamCh():
			t.Logf("stream found from n1")
			conn.Write([]byte("a"))
			conn.Write([]byte("b"))
			conn.Write([]byte("c"))

			var n int
			var hasAppended bool
			buf := make([]byte, 1500)
			require.Eventually(t, func() bool {
				m, err := readStreamSkipEmpty(t, ctx, stream, buf[n:])
				n = m + n
				if !hasAppended {
					conn.Write([]byte("d"))
					hasAppended = true
				}
				current := string(buf[:n])
				t.Logf("currently the buffer contains: %s", current)
				return err == nil && current == "abcd"
			}, 2*time.Second, 100*time.Millisecond)
		case <-ctx.Done():
			t.Fatalf("timed out")
		}
		cancel()
	})

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		ts1.Shutdown()
		wg.Done()
	}()
	go func() {
		ts2.Shutdown()
		wg.Done()
	}()
	wg.Wait()
}

func readStreamSkipEmpty(t *testing.T, ctx context.Context, stream net.Conn, buf []byte) (int, error) {
	var n int
	var err error
	dl, ok := ctx.Deadline()
	if ok {
		stream.SetReadDeadline(dl)
	}

	for {
		n, err = stream.Read(buf)
		if err != nil {
			return 0, err
		}

		if ctx.Err() != nil {
			return 0, ctx.Err()
		}

		if n > 0 {
			return n, nil
		} else {
			t.Log("received an empty frame from the stream")
		}
	}
}

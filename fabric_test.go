package grinta

import (
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFabric(t *testing.T) {
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

	fbNode1, err := Create(
		WithHostname("node1"),
		WithListenOn("127.0.0.1", 6021),
		WithLog(n1handler),
		WithTlsConfig(tcN1),
		WithNeighbours([]string{"localhost:6022"}),
		WithMetricSink(nil),
	)
	if err != nil {
		t.Fatalf("failed to start node1: %s", err)
		return
	}

	fbNode2, err := Create(
		WithHostname("node2"),
		WithListenOn("127.0.0.1", 6022),
		WithLog(n2handler),
		WithTlsConfig(tcN2),
		WithNeighbours([]string{"localhost:6021"}),
		WithMetricSink(nil),
	)
	if err != nil {
		t.Fatalf("failed to start node2: %s", err)
		return
	}

	t.Run("when node1 join node2, node2 can see node1 info", func(t *testing.T) {
		require.NoError(t, fbNode1.JoinCluster())
		require.Eventually(t, func() bool {
			for _, mem := range fbNode2.Topology() {
				if mem.Name == "node1" {
					return true
				}
			}
			return false
		}, 10*time.Second, 1*time.Second)
	})
}

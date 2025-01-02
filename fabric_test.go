package grinta

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type TestString string

func (ts TestString) Marshal() ([]byte, error) {
	return []byte(ts), nil
}

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
	leafKey := generateKeyPair(t)

	caDER := generateCa(t, caKey)
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("failed to parse CA: %s", err)
		return
	}

	leafDER := generateLeaf(t, ca, caKey, leafKey, "leaf")
	leafCert, err := x509.ParseCertificate(leafDER)
	if err != nil {
		t.Fatalf("failed to parse node1: %s", err)
		return
	}

	caPool := x509.NewCertPool()
	caPool.AddCert(ca)

	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{
			{
				Certificate: [][]byte{leafDER},
				Leaf:        leafCert,
				PrivateKey:  leafKey,
			},
		},
		ClientAuth: tls.RequireAndVerifyClientCert,
		ClientCAs:  caPool,
		RootCAs:    caPool,
	}

	fbNode1, err := Create(
		WithNodeName("node1"),
		WithListenOn("127.0.0.1", 6021),
		WithLog(n1handler),
		WithTlsConfig(tlsConf),
		WithNeighbours([]string{"localhost:6022"}),
		WithMetricSink(nil),
	)
	if err != nil {
		t.Fatalf("failed to start node1: %s", err)
		return
	}

	fbNode2, err := Create(
		WithNodeName("node2"),
		WithListenOn("127.0.0.1", 6022),
		WithLog(n2handler),
		WithTlsConfig(tlsConf),
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

	ep, err := fbNode1.CreateEndpoint("srv1")
	values := make(chan interface{})
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			rcv, err := ep.Accept(context.Background())
			if err != nil {
				t.Logf("error when accepting: %s", err)
				break
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					val, err := rcv.Read(context.Background())
					if err != nil {
						break
					}
					values <- val
				}
				rcv.Close()
			}()
		}
		ep.Close()
	}()

	t.Run("node1 can dial endpoint srv1 which is local", func(t *testing.T) {
		prod, err := fbNode1.DialEndpoint(context.Background(), "srv1")
		require.NoError(t, err)
		prod.Write(context.Background(), TestString("hello!"))
		val := <-values
		castedVal := val.(TestString)
		require.Equal(t, "hello!", string(castedVal))
	})

	fbNode1.Shutdown()
	fbNode2.Shutdown()
}

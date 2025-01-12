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

	"github.com/raskyld/grinta/pkg/flow"
	"github.com/stretchr/testify/assert"
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
	values := make(chan []byte)
	require.NoError(t, err)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			rcv, _, err := ep.Accept(context.Background())
			if err != nil {
				t.Logf("error when accepting: %s", err)
				break
			}

			flowRcv := flow.NewReceiver[[]byte](
				rcv,
				flow.NewBytesCodec(true),
				64,
			)

			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					val, err := flowRcv.Recv(context.Background())
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
		if !assert.NoError(t, err) {
			return
		}

		flowSend := flow.NewSender[[]byte](
			prod,
			flow.NewBytesCodec(true),
			64,
		)

		flowSend.Send(context.Background(), []byte("hello!"))
		val := <-values
		assert.Equal(t, "hello!", string(val))
	})

	t.Run("node2 can dial endpoint srv1 which is remote", func(t *testing.T) {
		prod, err := fbNode2.DialEndpoint(context.Background(), "srv1")
		if !assert.NoError(t, err) {
			return
		}

		flowSend := flow.NewSender[[]byte](
			prod,
			flow.NewBytesCodec(true),
			64,
		)

		flowSend.Send(context.Background(), []byte("hello remote!"))
		val := <-values
		assert.Equal(t, "hello remote!", string(val))
	})

	fbNode1.Shutdown()
	fbNode2.Shutdown()
}

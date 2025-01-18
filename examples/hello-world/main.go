package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/raskyld/grinta"
	"github.com/raskyld/grinta/pkg/flow"
)

var (
	Client = flag.Bool("client", false, "act as a client")
	Server = flag.Bool("server", false, "act as a server")

	Hostname   = flag.String("hostname", "", "hostname to advertise")
	Port       = flag.Int("port", 6174, "which port to bind")
	Neighbours = flag.String("neighbours", "", "comma-separated list of neighbours")

	TlsCert = flag.String("tls-cert", "", "client cert to use")
	TlsKey  = flag.String("tls-key", "", "client private key to use")
	TlsCA   = flag.String("tls-ca", "", "ca to verify neighbours")
)

func main() {
	flag.Parse()

	// mTLS is really important, so your nodes can authenticate to each other
	// and encrypt traffic.
	tlsConf, err := loadTlsConfig()
	if err != nil {
		slog.Error("failed to load tls creds", "error", err)
		os.Exit(1)
	}

	// Now, you can create your `grinta.Fabric` and customise it
	// using `grinta.Option`.
	neighbours := strings.Split(*Neighbours, ",")
	fabric, err := grinta.Create(
		grinta.WithNodeName(*Hostname),
		grinta.WithNeighbours(neighbours),
		grinta.WithTlsConfig(tlsConf),
		grinta.WithListenOn("127.0.0.1", *Port),
	)
	if err != nil {
		slog.Error("failed to create fabric", "error", err)
		os.Exit(2)
	}

	// This will actually initiate the cluster join process by
	// reaching the "neighbours" you provided.
	err = fabric.JoinCluster()
	if err != nil {
		slog.Error("failed to join cluster", "error", err)
		os.Exit(3)
	}

	// This will gracefully shutdown your node when pressing CTRL+C.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		<-sigCh
		slog.Info("terminating...")
		cancel(errors.New("user requested shutdown"))
	}()

	// And now we start our Goroutines!
	var wg sync.WaitGroup
	if *Server {
		wg.Add(1)
		go startServer(ctx, fabric, &wg)
	}
	if *Client {
		wg.Add(1)
		go startClient(ctx, fabric, &wg)
	}
	wg.Wait()

	fabric.Shutdown()
}

func startServer(ctx context.Context, fb *grinta.Fabric, wg *sync.WaitGroup) {
	defer wg.Done()

	// Allocate an endpoint named "server" on the fabric.
	endpoint, err := fb.CreateEndpoint("server")
	if err != nil {
		slog.Error("failed to allocate endpoint", "error", err)
		return
	}
	defer endpoint.Close()
	slog.Info("waiting for my hello world!")

	for {
		rawReceiver, _, err := endpoint.Accept(ctx)
		if err != nil {
			slog.Error("failed to accept flow establishment request", "error", err)
			break
		}
		slog.Info("established a flow")

		wg.Add(1)
		go func() {
			defer wg.Done()
			// For a good DX, we create a high-level "Receiver" flow,
			// which is buffured and strongly typed.
			rcv := flow.NewReceiver[[]byte](
				rawReceiver,
				// How to decode received message.
				flow.NewBytesCodec(true),
				// How many messages can be buffered.
				1024,
			)
			defer rcv.Close()
			concatStr := ""
			for {
				msg, err := rcv.Recv(ctx)
				if err != nil {
					slog.Error("failed to receive message", "error", err)
					break
				}
				slog.Info("received a message", "msg", msg)
				concatStr = concatStr + string(msg)

				if concatStr == "hello, world!" {
					slog.Info("I received a cute 'hello, world!' I can leave now!")
					fb.Shutdown()
					break
				}
			}
		}()
	}
}

func startClient(ctx context.Context, fb *grinta.Fabric, wg *sync.WaitGroup) {
	defer wg.Done()
	// We try to create a unidirectional flow to the endpoint named "server".
	rawSend, err := fb.DialEndpoint(ctx, "server")
	if err != nil {
		slog.Error("failed to reach endpoint server", "error", err)
		return
	}

	sender := flow.NewSender[[]byte](
		rawSend,
		// How to encode message.
		// true: means we copy the buffer when sending to local endpoints.
		flow.NewBytesCodec(true),
		// How many messages can be buffered.
		1024,
	)

	err = sender.Send(ctx, []byte("hello"))
	if err != nil {
		slog.Error("failed to send message", "error", err)
		return
	}
	slog.Info("I sent the hello")

	time.Sleep(2 * time.Second)

	err = sender.Send(ctx, []byte(", world!"))
	if err != nil {
		slog.Error("failed to send message", "error", err)
		return
	}
	slog.Info("I sent the world!")
}

func loadTlsConfig() (*tls.Config, error) {
	if *TlsCA == "" || *TlsCert == "" || *TlsKey == "" {
		return nil, errors.New("all tls option must be provided")
	}

	keypair, err := tls.LoadX509KeyPair(*TlsCert, *TlsKey)
	if err != nil {
		return nil, fmt.Errorf("failed to load client cert: %w", err)
	}

	caBytes, err := os.ReadFile(*TlsCA)
	if err != nil {
		return nil, fmt.Errorf("failed to load CA: %w", err)
	}

	caBundle := x509.NewCertPool()
	caBundle.AppendCertsFromPEM(caBytes)

	return &tls.Config{
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caBundle,
		Certificates: []tls.Certificate{keypair},
		RootCAs:      caBundle,
	}, nil
}

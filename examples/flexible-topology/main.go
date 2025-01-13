package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"os"
	"os/signal"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/raskyld/grinta"
	"github.com/raskyld/grinta/pkg/flow"
)

var (
	From  = flag.Int("from", 1, "host node starting with this number")
	Until = flag.Int("until", 2, "host node until this number")
	Max   = flag.Int("max", 1, "maximal node number to try to communicate with")

	Hostname   = flag.String("hostname", "", "hostname to advertise")
	Port       = flag.Int("port", 6174, "which port to bind")
	Neighbours = flag.String("neighbours", "", "comma-separated list of neighbours")

	TlsCert = flag.String("tls-cert", "", "client cert to use")
	TlsKey  = flag.String("tls-key", "", "client private key to use")
	TlsCA   = flag.String("tls-ca", "", "ca to verify neighbours")
)

type msg struct {
	From    string
	Content string
	At      time.Time
}

var _ flow.Clonable = (*msg)(nil)

func (m *msg) Clone() interface{} {
	return &msg{
		From:    m.From,
		Content: m.Content,
		At:      m.At,
	}
}

func main() {
	flag.Parse()
	tlsConf, err := loadTlsConfig()
	if err != nil {
		slog.Error("failed to load tls creds", "error", err)
		os.Exit(1)
	}

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

	err = fabric.JoinCluster()
	if err != nil {
		slog.Error("failed to join cluster", "error", err)
		os.Exit(3)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	ctx, cancel := context.WithCancelCause(context.Background())

	go func() {
		<-sigCh
		slog.Info("terminating...")
		cancel(errors.New("user requested shutdown"))
	}()

	cpuProFile, err := os.Create(*Hostname + ".prof")
	if err != nil {
		slog.Error("failed to create cpu profile file", "error", err)
		os.Exit(3)
	}

	if err := pprof.StartCPUProfile(cpuProFile); err != nil {
		slog.Error("failed to start cpu profile", "error", err)
		os.Exit(3)
	}
	defer pprof.StopCPUProfile()

	var wg sync.WaitGroup
	for i := *From; i < *Until; i++ {
		wg.Add(2)
		go startMicroprocessRead(ctx, fabric, &wg, i)
		go startMicroprocessWrite(ctx, fabric, &wg, i)
	}
	wg.Wait()

	fabric.Shutdown()
}

func startMicroprocessWrite(ctx context.Context, fb *grinta.Fabric, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	flows := make(map[string]*flow.Sender[*msg])
	ticker := time.NewTicker(time.Duration(rand.IntN(4)+1) * time.Second)
	for {
		select {
		case <-ticker.C:
			var destId int
			for destId == 0 || destId == id {
				destId = rand.IntN(*Max) + 1
			}

			dest := strconv.Itoa(destId)
			slog.Info("send a message", "dest", dest, "from", id)
			fl, ok := flows[dest]
			if !ok {
				slog.Info("must open a new flow", "dest", dest, "from", id)
				dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				rawSend, err := fb.DialEndpoint(dialCtx, dest)
				if err != nil {
					slog.Error("failed to dial", "dest", dest, "error", err)
					continue
				}
				fl = flow.NewSender[*msg](
					rawSend,
					flow.NewJsonEncoder(false),
					64,
				)
				flows[dest] = fl
			}
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			err := fl.Send(writeCtx, &msg{
				From:    strconv.Itoa(id),
				Content: fmt.Sprintf("hello from %d", id),
				At:      time.Now(),
			})
			if err != nil {
				slog.Error("failed to send message", "dest", dest, "error", err)
				if errors.Is(err, net.ErrClosed) {
					slog.Info("trying to open a new flow")
					dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
					defer cancel()
					rawSend, err := fb.DialEndpoint(dialCtx, dest)
					if err != nil {
						slog.Error("failed to dial", "dest", dest, "error", err)
						continue
					}
					fl = flow.NewSender[*msg](
						rawSend,
						flow.NewJsonEncoder(false),
						64,
					)
					flows[dest] = fl
				}
				continue
			}
		case <-ctx.Done():
			slog.Info("stop sending msg", "from", id)
			return
		}
	}
}

func startMicroprocessRead(ctx context.Context, fb *grinta.Fabric, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	var ep grinta.Endpoint
	for ep == nil {
		if ctx.Err() != nil {
			return
		}
		endpoint, err := fb.CreateEndpoint(strconv.Itoa(id))
		if err != nil {
			slog.Error("failed to allocate endpoint", "id", id, "error", err)
			time.Sleep(2 * time.Second)
			continue
		}
		ep = endpoint
	}

	for {
		rawRcv, _, err := ep.Accept(ctx)
		if err != nil {
			slog.Error("stop accepting flows", "id", id, "error", err)
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			rcv := flow.NewReceiver[*msg](
				rawRcv,
				flow.NewJsonDecoder[*msg](),
				64,
			)
			for {
				msg, err := rcv.Recv(ctx)
				if err != nil {
					slog.Error("error reading flow", "error", err)
					return
				}
				slog.Info(
					"message received",
					"from", msg.From,
					"content", msg.Content,
					"id", id,
					"latency", time.Since(msg.At),
				)
			}
		}()
	}
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

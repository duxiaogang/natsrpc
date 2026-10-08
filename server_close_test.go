package natsrpc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestServerCloseWithoutDeadline(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)
	registerTestGreeting(t, server, "close")
	if err := server.Close(context.Background()); err != nil {
		t.Fatalf("Close(context.Background()) error = %v", err)
	}
	requireNoGreeting(t, NewClient(conn))
}

// TestServerCloseUnsubscribesEvenWithExpiredContext 覆盖回归场景：调用 Close 时
// 传入的 ctx 已经过期/已取消，取消订阅仍必须执行，不能被直接跳过。
func TestServerCloseUnsubscribesEvenWithExpiredContext(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)
	registerTestGreeting(t, server, "close")

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	cancel()
	<-ctx.Done()

	if err := server.Close(ctx); !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close(expired ctx) error = %v, want context.Canceled or context.DeadlineExceeded", err)
	}
	requireNoGreeting(t, NewClient(conn))
}

func TestServerCloseHonorsContextDuringFlush(t *testing.T) {
	for _, tt := range []struct {
		name     string
		register bool
		deadline bool
	}{
		{name: "unsubscribe flush deadline", register: true, deadline: true},
		{name: "unsubscribe flush cancellation", register: true},
		{name: "final flush cancellation"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, gate := newCloseTestNATSConn(t)
			server, err := NewServer(conn)
			if err != nil {
				t.Fatal(err)
			}
			if tt.register {
				registerTestGreeting(t, server, "close")
			}
			atomic.StoreInt32(&gate.enabled, 1)
			defer gate.resume()

			var ctx context.Context
			var cancel context.CancelFunc
			wantErr := context.Canceled
			if tt.deadline {
				ctx, cancel = context.WithTimeout(context.Background(), 500*time.Millisecond)
				wantErr = context.DeadlineExceeded
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			closed := make(chan error, 1)
			go func() { closed <- server.Close(ctx) }()
			select {
			case <-gate.entered:
			case err := <-closed:
				t.Fatalf("Close returned before flushing: %v", err)
			case <-time.After(2 * time.Second):
				t.Fatal("Close did not send a flush PING")
			}
			if !tt.deadline {
				cancel()
			}
			<-ctx.Done()
			select {
			case err := <-closed:
				if !errors.Is(err, wantErr) {
					t.Fatalf("Close() error = %v, want %v", err, wantErr)
				}
			case <-time.After(500 * time.Millisecond):
				gate.resume()
				t.Fatal("Close remained blocked after its context ended")
			}
		})
	}
}

func TestServerCloseHonorsContextWhileRegistryLocked(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)
	// Register/Remove can hold this lock while waiting for a NATS Flush.
	server.mu.Lock()
	defer server.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	closed := make(chan error, 1)
	go func() { closed <- server.Close(ctx) }()
	<-ctx.Done()
	select {
	case err := <-closed:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Close() error = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close remained blocked on the registry lock after its deadline")
	}
}

func TestServerCloseHonorsContextWhileHandlerRunning(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)
	server.wg.Add(1)
	defer server.wg.Done()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := server.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Close() error = %v, want context.DeadlineExceeded", err)
	}
}

type closeTestPongGate struct {
	enabled int32
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *closeTestPongGate) resume() {
	g.once.Do(func() { close(g.release) })
}

// newCloseTestNATSConn provides only the NATS handshake and PING/PONG exchange.
// Delaying PONG lets tests check Flush cancellation without network timing races.
func newCloseTestNATSConn(t *testing.T) (*nats.Conn, *closeTestPongGate) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gate := &closeTestPongGate{entered: make(chan struct{}, 1), release: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		peer, err := listener.Accept()
		if err != nil {
			return
		}
		defer peer.Close()
		if _, err := fmt.Fprint(peer, "INFO {\"server_id\":\"close-test\",\"version\":\"2.8.4\",\"proto\":1,\"headers\":true,\"max_payload\":1048576}\r\n"); err != nil {
			return
		}
		scanner := bufio.NewScanner(peer)
		for scanner.Scan() {
			if strings.TrimSpace(scanner.Text()) != "PING" {
				continue
			}
			if atomic.LoadInt32(&gate.enabled) != 0 {
				select {
				case gate.entered <- struct{}{}:
				default:
				}
				<-gate.release
			}
			if _, err := fmt.Fprint(peer, "PONG\r\n"); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		gate.resume()
		listener.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("test NATS peer did not stop")
		}
	})
	conn, err := nats.Connect("nats://"+listener.Addr().String(), nats.NoReconnect(), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Close)
	return conn, gate
}

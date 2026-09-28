package natsrpc

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestServerRequestQueueAndPublishBroadcast(t *testing.T) {
	for _, tt := range []struct {
		name    string
		publish bool
		copies  int
	}{
		{"request handled once", false, 1},
		{"publish reaches every instance", true, 3},
	} {
		t.Run(tt.name, func(t *testing.T) {
			const requests = 12
			conn := newTestNATSConn(t)
			delivered := make(chan string, requests*3)
			desc := ServiceDesc{
				ServiceName: "natsrpc.test.Delivery",
				Methods: []MethodDesc{{
					MethodName:     "Hello",
					IsPublish:      tt.publish,
					RequestFactory: func() any { return &serverTestRequest{} },
					Handler: func(_ interface{}, _ context.Context, req interface{}) (interface{}, error) {
						name := req.(*serverTestRequest).Name
						delivered <- name
						return &serverTestReply{Message: name}, nil
					},
				}},
			}
			for i := 0; i < 3; i++ {
				server := newTestRPCServer(t, conn)
				if _, err := server.Register(desc, nil); err != nil {
					t.Fatal(err)
				}
			}
			client := NewClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for i := 0; i < requests; i++ {
				req := &serverTestRequest{Name: strconv.Itoa(i)}
				var err error
				if tt.publish {
					err = client.Publish(desc.ServiceName, "Hello", req)
				} else {
					err = client.Request(ctx, desc.ServiceName, "Hello", req, &serverTestReply{})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			flushTestCallbacks(t, conn)
			counts := map[string]int{}
			for len(delivered) > 0 {
				counts[<-delivered]++
			}
			for i := 0; i < requests; i++ {
				if got := counts[strconv.Itoa(i)]; got != tt.copies {
					t.Errorf("message %d handled %d times, want %d", i, got, tt.copies)
				}
			}
		})
	}
}

func TestServerRegisterRollsBackRejectedSubscriptions(t *testing.T) {
	conn := newTestNATSConn(t)
	server, err := NewServer(conn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil && !errors.Is(err, nats.ErrConnectionClosed) {
			t.Error(err)
		}
	})
	before := conn.NumSubscriptions()
	// The request wildcard is valid, but appending the publish suffix is invalid.
	// NATS rejects the subscription by closing the connection during Flush.
	_, err = server.Register(ServiceDesc{
		ServiceName: "natsrpc.test.>",
		Methods:     []MethodDesc{{MethodName: "Notify", IsPublish: true}},
	}, nil)
	if err == nil {
		t.Fatal("Register() succeeded with an invalid publish subject")
	}
	if got := conn.NumSubscriptions(); got != before {
		t.Fatalf("subscription count = %d, want %d", got, before)
	}
	if len(server.services) != 0 {
		t.Fatal("failed registration left a service entry")
	}
}

func flushTestCallbacks(t *testing.T, conn *nats.Conn) {
	t.Helper()
	if err := conn.FlushTimeout(2 * time.Second); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	if err := conn.Barrier(func() { close(done) }); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription callbacks did not finish")
	}
}

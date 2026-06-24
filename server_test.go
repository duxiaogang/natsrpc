package natsrpc

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	goproto "github.com/gogo/protobuf/proto"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

type serverTestRequest struct {
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (m *serverTestRequest) Reset()         { *m = serverTestRequest{} }
func (m *serverTestRequest) String() string { return goproto.CompactTextString(m) }
func (*serverTestRequest) ProtoMessage()    {}

type serverTestReply struct {
	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
}

func (m *serverTestReply) Reset()         { *m = serverTestReply{} }
func (m *serverTestReply) String() string { return goproto.CompactTextString(m) }
func (*serverTestReply) ProtoMessage()    {}

type serverTestGreeting struct {
	id string
}

func (s *serverTestGreeting) Hello(ctx context.Context, req *serverTestRequest) (*serverTestReply, error) {
	return &serverTestReply{Message: s.id + ":" + req.Name}, nil
}

var serverTestGreetingDesc = ServiceDesc{
	ServiceName: "natsrpc.test.Greeting",
	Methods: []MethodDesc{
		{
			MethodName: "Hello",
			Handler: func(svc interface{}, ctx context.Context, req interface{}) (interface{}, error) {
				return svc.(*serverTestGreeting).Hello(ctx, req.(*serverTestRequest))
			},
			RequestType: reflect.TypeOf(serverTestRequest{}),
		},
	},
}

func TestServerRegisterSameServiceDifferentIDsAndCloseOne(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)
	client := NewClient(conn)

	svc1 := registerTestGreeting(t, server, "role-1", WithServiceID("role-1"))
	svc2 := registerTestGreeting(t, server, "role-2", WithServiceID("role-2"))

	requireGreeting(t, client, "role-1:gopher", WithCallID("role-1"))
	requireGreeting(t, client, "role-2:gopher", WithCallID("role-2"))

	if !svc1.Close() {
		t.Fatal("Close() for role-1 returned false")
	}
	requireNoGreeting(t, client, WithCallID("role-1"))
	requireGreeting(t, client, "role-2:gopher", WithCallID("role-2"))

	if !svc2.Close() {
		t.Fatal("Close() for role-2 returned false")
	}
	requireNoGreeting(t, client, WithCallID("role-2"))
}

func TestServerRegisterDuplicateServiceID(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)

	registerTestGreeting(t, server, "role-1", WithServiceID("role-1"))

	_, err := server.Register(serverTestGreetingDesc, &serverTestGreeting{id: "duplicate"}, WithServiceID("role-1"))
	if !errors.Is(err, ErrDuplicateService) {
		t.Fatalf("Register duplicate error = %v, want %v", err, ErrDuplicateService)
	}
}

func TestServerRegisterWithoutIDKeepsOldBehavior(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)
	client := NewClient(conn)

	svc := registerTestGreeting(t, server, "default")

	requireGreeting(t, client, "default:gopher")

	_, err := server.Register(serverTestGreetingDesc, &serverTestGreeting{id: "duplicate"})
	if !errors.Is(err, ErrDuplicateService) {
		t.Fatalf("Register duplicate error = %v, want %v", err, ErrDuplicateService)
	}
	if !svc.Close() {
		t.Fatal("Close() without id returned false")
	}
	requireNoGreeting(t, client)
}

func TestServerRegisterWithNamespaceClosesByFullSubject(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)
	client := NewClient(conn, WithClientNamespace("ns"))

	svc := registerTestGreeting(t, server, "namespaced", WithServiceNamespace("ns"))

	requireGreeting(t, client, "namespaced:gopher")
	if !svc.Close() {
		t.Fatal("Close() with namespace returned false")
	}
	requireNoGreeting(t, client)
}

func TestServerUnSubscribeAllClearsAllServiceIDs(t *testing.T) {
	conn := newTestNATSConn(t)
	server := newTestRPCServer(t, conn)
	client := NewClient(conn)

	registerTestGreeting(t, server, "role-1", WithServiceID("role-1"))
	registerTestGreeting(t, server, "role-2", WithServiceID("role-2"))

	requireGreeting(t, client, "role-1:gopher", WithCallID("role-1"))
	requireGreeting(t, client, "role-2:gopher", WithCallID("role-2"))

	if err := server.UnSubscribeAll(); err != nil {
		t.Fatalf("UnSubscribeAll() error = %v", err)
	}
	requireNoGreeting(t, client, WithCallID("role-1"))
	requireNoGreeting(t, client, WithCallID("role-2"))
}

func newTestNATSConn(t *testing.T) *nats.Conn {
	t.Helper()

	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:   "127.0.0.1",
		Port:   natsserver.RANDOM_PORT,
		NoLog:  true,
		NoSigs: true,
	})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(10 * time.Second) {
		ns.Shutdown()
		t.Fatal("nats server did not become ready")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})

	conn, err := nats.Connect(ns.ClientURL(), nats.NoReconnect(), nats.Timeout(2*time.Second))
	if err != nil {
		t.Fatalf("nats.Connect() error = %v", err)
	}
	t.Cleanup(conn.Close)
	return conn
}

func newTestRPCServer(t *testing.T, conn *nats.Conn) *Server {
	t.Helper()

	server, err := NewServer(conn)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := server.Close(ctx); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	})
	return server
}

func registerTestGreeting(t *testing.T, server *Server, id string, opts ...ServiceOption) ServiceInterface {
	t.Helper()

	svc, err := server.Register(serverTestGreetingDesc, &serverTestGreeting{id: id}, opts...)
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return svc
}

func requireGreeting(t *testing.T, client *Client, want string, opts ...CallOption) {
	t.Helper()

	got, err := requestGreeting(t, client, 2*time.Second, opts...)
	if err != nil {
		t.Fatalf("Request() error = %v", err)
	}
	if got != want {
		t.Fatalf("Request() message = %q, want %q", got, want)
	}
}

func requireNoGreeting(t *testing.T, client *Client, opts ...CallOption) {
	t.Helper()

	if got, err := requestGreeting(t, client, 200*time.Millisecond, opts...); err == nil {
		t.Fatalf("Request() message = %q, want error", got)
	}
}

func requestGreeting(t *testing.T, client *Client, timeout time.Duration, opts ...CallOption) (string, error) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	reply := &serverTestReply{}
	err := client.Request(ctx, serverTestGreetingDesc.ServiceName, "Hello", &serverTestRequest{Name: "gopher"}, reply, opts...)
	if err != nil {
		return "", err
	}
	return reply.Message, nil
}

package natsrpc

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClientNewCallOptions(t *testing.T) {
	c := &Client{
		opt: ClientOptions{
			id: "client-default",
		},
	}

	tests := []struct {
		name string
		opt  []CallOption
		want string
	}{
		{
			name: "inherit client default id",
			want: "client-default",
		},
		{
			name: "override with call id",
			opt:  []CallOption{WithCallID("call-id")},
			want: "call-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.newCallOptions(tt.opt...).id
			if got != tt.want {
				t.Fatalf("newCallOptions().id = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClientSubject(t *testing.T) {
	c := &Client{
		opt: ClientOptions{
			namespace: "ns",
		},
	}

	tests := []struct {
		name      string
		isPublish bool
		id        string
		want      string
	}{
		{
			name:      "request without id",
			isPublish: false,
			want:      "ns.svc",
		},
		{
			name:      "request with id",
			isPublish: false,
			id:        "1",
			want:      "ns.svc.1",
		},
		{
			name:      "publish with id",
			isPublish: true,
			id:        "1",
			want:      "ns.svc.1._nr_pub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := c.subject("svc", tt.isPublish, tt.id)
			if got != tt.want {
				t.Fatalf("subject() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestClientRequestDecodesEmptyResponse(t *testing.T) {
	decodeErr := errors.New("empty response rejected by decoder")
	for _, tt := range []struct {
		name    string
		encoder Encoder
		wantErr error
	}{
		{name: "protobuf clears previous fields", encoder: defaultEncoder},
		{
			name: "custom decoder error is returned",
			encoder: clientTestErrorDecoder{
				Encoder: defaultEncoder,
				err:     decodeErr,
			},
			wantErr: decodeErr,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTestNATSConn(t)
			server := newTestRPCServer(t, conn)
			desc := ServiceDesc{
				ServiceName: "natsrpc.test.EmptyResponse",
				Methods: []MethodDesc{{
					MethodName:     "Hello",
					RequestFactory: func() any { return &serverTestRequest{} },
					Handler: func(_ interface{}, _ context.Context, _ interface{}) (interface{}, error) {
						return &serverTestReply{}, nil
					},
				}},
			}
			if _, err := server.Register(desc, nil); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			reply := &serverTestReply{Message: "previous response"}
			client := NewClient(conn, WithClientEncoder(tt.encoder))
			err := client.Request(ctx, desc.ServiceName, "Hello", &serverTestRequest{}, reply)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Request() error = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && reply.Message != "" {
				t.Fatalf("empty response retained previous Message = %q", reply.Message)
			}
		})
	}
}

type clientTestErrorDecoder struct {
	Encoder
	err error
}

func (e clientTestErrorDecoder) Decode(_ []byte, _ interface{}) error {
	return e.err
}

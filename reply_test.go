package natsrpc

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDelayedReplyPreservesErrorsAndHeaders(t *testing.T) {
	for _, tt := range []struct {
		name        string
		reply       any
		replyErr    error
		wantMessage string
		wantError   string
	}{
		{name: "success", reply: &serverTestReply{Message: "later"}, wantMessage: "later"},
		{name: "empty success"},
		{name: "nil reply with business error", replyErr: errors.New("denied"), wantError: "denied"},
		{name: "business error skips encoding", reply: struct{}{}, replyErr: errors.New("denied"), wantError: "denied"},
		{name: "encoding error", reply: struct{}{}, wantError: "encode response failed:"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := newTestNATSConn(t)
			server := newTestRPCServer(t, conn)
			ready := make(chan context.Context, 1)
			desc := ServiceDesc{
				ServiceName: "natsrpc.test.DelayedReply",
				Methods: []MethodDesc{{
					MethodName:     "Hello",
					RequestFactory: func() any { return &serverTestRequest{} },
					Handler: func(_ interface{}, ctx context.Context, _ interface{}) (interface{}, error) {
						if err := SetReplyHeader(ctx, map[string]string{"trace": "reply-trace"}); err != nil {
							return nil, err
						}
						ready <- ctx
						return nil, ErrReplyLater
					},
				}},
			}
			if _, err := server.Register(desc, nil); err != nil {
				t.Fatal(err)
			}
			type result struct {
				reply  serverTestReply
				header map[string]string
				err    error
			}
			results := make(chan result, 1)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go func() {
				var got result
				got.err = NewClient(conn).Request(ctx, desc.ServiceName, "Hello", &serverTestRequest{}, &got.reply, WithCallReplyHeader(&got.header))
				results <- got
			}()
			var handlerCtx context.Context
			select {
			case handlerCtx = <-ready:
			case <-ctx.Done():
				t.Fatal("handler did not receive request")
			}
			// Wait for the handler to return ErrReplyLater before replying.
			flushTestCallbacks(t, conn)
			if err := handlerCtx.Err(); err != nil {
				t.Fatalf("delayed reply context was canceled early: %v", err)
			}
			reply := MakeReplyFunc[any](handlerCtx)
			if err := reply(tt.reply, tt.replyErr); err != nil {
				t.Fatal(err)
			}
			if err := handlerCtx.Err(); !errors.Is(err, context.Canceled) {
				t.Fatalf("reply did not release its context: %v", err)
			}
			select {
			case got := <-results:
				if tt.wantError == "" {
					if got.err != nil {
						t.Fatal(got.err)
					}
				} else if got.err == nil || !strings.Contains(got.err.Error(), tt.wantError) {
					t.Fatalf("client error = %v, want %q", got.err, tt.wantError)
				}
				if got.reply.Message != tt.wantMessage || got.header["trace"] != "reply-trace" {
					t.Fatalf("reply=%+v header=%v", got.reply, got.header)
				}
			case <-ctx.Done():
				t.Fatal("client did not receive delayed reply")
			}
		})
	}
}

func TestReplyHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Reply(ctx, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Reply() error = %v, want context.Canceled", err)
	}
}

package natsrpc

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestReplyHeaderRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{
			name: "populated",
			in:   map[string]string{"x-a": "1", "x-b": "2"},
			want: map[string]string{"x-a": "1", "x-b": "2"},
		},
		{
			name: "empty map emits no key",
			in:   map[string]string{},
			want: nil,
		},
		{
			name: "nil map emits no key",
			in:   nil,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := addReplyHeader(nil, tt.in)
			got := decodeReplyHeader(h)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("round trip = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDecodeReplyHeaderOddLength(t *testing.T) {
	h := nats.Header{headerReply: {"only-key"}}
	if got := decodeReplyHeader(h); got != nil {
		t.Fatalf("odd-length decode = %v, want nil", got)
	}
}

// TestReplyHeaderKeyIsolation 确认 _ns_reply 与 _ns_user/_ns_method/_ns_error
// 同时存在于一个 nats.Header 上时互不串台。
func TestReplyHeaderKeyIsolation(t *testing.T) {
	h, err := encodeHeader("DoSomething", map[string]string{"req-k": "req-v"})
	if err != nil {
		t.Fatalf("encodeHeader: %v", err)
	}
	h = addReplyHeader(h, map[string]string{"rep-k": "rep-v"})
	h[headerError] = []string{"some-error"}

	method, reqHeader, err := decodeHeader(h)
	if err != nil {
		t.Fatalf("decodeHeader: %v", err)
	}
	if method != "DoSomething" {
		t.Errorf("method = %q, want DoSomething", method)
	}
	if got := reqHeader["req-k"]; got != "req-v" {
		t.Errorf("req header = %q, want req-v", got)
	}
	if _, ok := reqHeader["rep-k"]; ok {
		t.Errorf("reply key leaked into request header")
	}

	repHeader := decodeReplyHeader(h)
	if got := repHeader["rep-k"]; got != "rep-v" {
		t.Errorf("reply header = %q, want rep-v", got)
	}
	if _, ok := repHeader["req-k"]; ok {
		t.Errorf("request key leaked into reply header")
	}

	if got := getErrorHeader(h); got != "some-error" {
		t.Errorf("error header = %q, want some-error", got)
	}
}

func TestSetReplyHeader(t *testing.T) {
	t.Run("no meta returns ErrNoMeta", func(t *testing.T) {
		err := SetReplyHeader(context.Background(), map[string]string{"k": "v"})
		if !errors.Is(err, ErrNoMeta) {
			t.Fatalf("err = %v, want ErrNoMeta", err)
		}
	})

	t.Run("empty header is a no-op", func(t *testing.T) {
		// 即使没有 meta，空 header 也应直接返回 nil（不报 ErrNoMeta）。
		if err := SetReplyHeader(context.Background(), nil); err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
	})

	t.Run("merges across calls", func(t *testing.T) {
		meta := &metaValue{}
		ctx := withMeta(context.Background(), meta)
		if err := SetReplyHeader(ctx, map[string]string{"a": "1"}); err != nil {
			t.Fatalf("first set: %v", err)
		}
		if err := SetReplyHeader(ctx, map[string]string{"b": "2"}); err != nil {
			t.Fatalf("second set: %v", err)
		}
		got := meta.snapshotReplyHeader()
		want := map[string]string{"a": "1", "b": "2"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("snapshot = %v, want %v", got, want)
		}
	})
}

func TestWithCallReplyHeader(t *testing.T) {
	var rh map[string]string
	opt := &CallOptions{}
	WithCallReplyHeader(&rh)(opt)
	if opt.replyHeader != &rh {
		t.Fatalf("replyHeader pointer not set to provided address")
	}
}

package natsrpc

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
)

const (
	headerMethod = "_ns_method" // method
	headerUser   = "_ns_user"   // user header
	headerError  = "_ns_error"  // reply error
	headerReply  = "_ns_reply"  // reply user header
)

type metaKey struct{}
type metaValue struct {
	header map[string]string
	reply  string
	server *Server

	mu          sync.Mutex        // 守护 replyHeader（延迟回复时跨 goroutine 访问）
	replyHeader map[string]string // 服务端 handler/interceptor 写入，回传给客户端
}

func withMeta(ctx context.Context, meta *metaValue) context.Context {
	newCtx := context.WithValue(ctx, metaKey{}, meta)
	return newCtx
}

func getMeta(ctx context.Context) *metaValue {
	if ctx == nil {
		return nil
	}
	val := ctx.Value(metaKey{})
	if val == nil {
		return nil
	}
	meta, _ := val.(*metaValue)
	return meta
}

// CallHeader 获得call Header
func CallHeader(ctx context.Context) map[string]string {
	meta := getMeta(ctx)
	if meta == nil {
		return nil
	}
	return meta.header
}

// SetReplyHeader 把响应 header 合并进当前调用的 meta，服务端会随响应回传给客户端。
// 多次调用按 key 合并（后写覆盖同名 key），因此多个中间件可以各自追加。
// 当 ctx 中没有 meta（即不在一次 RPC 调用内）时返回 ErrNoMeta。
func SetReplyHeader(ctx context.Context, header map[string]string) error {
	if len(header) == 0 {
		return nil
	}
	meta := getMeta(ctx)
	if meta == nil {
		return ErrNoMeta
	}
	meta.mu.Lock()
	if meta.replyHeader == nil {
		meta.replyHeader = make(map[string]string, len(header))
	}
	for k, v := range header {
		meta.replyHeader[k] = v
	}
	meta.mu.Unlock()
	return nil
}

// snapshotReplyHeader 在持锁下拷贝一份 replyHeader，供构造响应消息时使用。
func (m *metaValue) snapshotReplyHeader() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.replyHeader) == 0 {
		return nil
	}
	out := make(map[string]string, len(m.replyHeader))
	for k, v := range m.replyHeader {
		out[k] = v
	}
	return out
}

func encodeHeader(method string, header map[string]string) (nats.Header, error) {
	ret := map[string][]string{headerMethod: {method}}
	if len(header) > 0 {
		ret[headerUser] = make([]string, 0, len(header)*2)
		for k, v := range header {
			ret[headerUser] = append(ret[headerUser], k, v)
		}
	}
	return ret, nil
}

func decodeHeader(h nats.Header) (method string, header map[string]string, err error) {
	val := h[headerMethod]
	if len(val) == 0 {
		return "", nil, ErrHeaderFormat
	}
	method = val[0]

	if kv := h[headerUser]; len(kv) > 0 && len(kv)%2 == 0 {
		header = make(map[string]string)
		for i := 0; i < len(kv); i += 2 {
			header[kv[i]] = kv[i+1]
		}
	}
	return
}

func makeErrorHeader(err error) nats.Header {
	if err != nil {
		return map[string][]string{headerError: {err.Error()}}
	}
	return nil
}

func getErrorHeader(h nats.Header) string {
	if h == nil {
		return ""
	}
	val := h[headerError]
	if len(val) == 0 {
		return ""
	}
	return val[0]
}

// addReplyHeader 把 reply header 以扁平 k,v 形式写入 headerReply 字段。
// h 可能为 nil（成功响应没有 error header），此时按需新建。
func addReplyHeader(h nats.Header, header map[string]string) nats.Header {
	if len(header) == 0 {
		return h
	}
	if h == nil {
		h = nats.Header{}
	}
	kv := make([]string, 0, len(header)*2)
	for k, v := range header {
		kv = append(kv, k, v)
	}
	h[headerReply] = kv
	return h
}

// decodeReplyHeader 从 headerReply 字段还原 reply header；缺失或格式非法时返回 nil。
func decodeReplyHeader(h nats.Header) map[string]string {
	if h == nil {
		return nil
	}
	kv := h[headerReply]
	if len(kv) == 0 || len(kv)%2 != 0 {
		return nil
	}
	header := make(map[string]string, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		header[kv[i]] = kv[i+1]
	}
	return header
}

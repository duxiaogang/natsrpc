package natsrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type serviceWrapper struct {
	*Service
	subscriptions []*nats.Subscription
}

var _ ServiceRegistrar = (*Server)(nil)

// Server RPC server
type Server struct {
	wg       sync.WaitGroup             // wait group
	mu       sync.Mutex                 // lock
	opt      ServerOptions              // options
	conn     *nats.Conn                 // NATS Encode Conn
	services map[string]*serviceWrapper // 服务 name->Service
	Encoder
}

// NewServer 构造器
func NewServer(conn *nats.Conn, option ...ServerOption) (*Server, error) {
	if !conn.IsConnected() {
		return nil, fmt.Errorf("conn is not connected")
	}

	options := DefaultServerOptions
	for _, v := range option {
		v(&options)
	}

	d := &Server{
		opt:      options,
		conn:     conn,
		services: map[string]*serviceWrapper{},
		Encoder:  options.encoder,
	}
	return d, nil
}

// Close 取消订阅，等待正在执行的 handler 结束并刷新连接。
// 取消订阅这一步始终执行，即使 ctx 已过期/已取消，避免调用方传入已过期 ctx 时
// 订阅完全不被清理；ctx 只用于控制等待 handler 结束和 flush 这两个可能阻塞的阶段。
func (s *Server) Close(ctx context.Context) error {
	// Register/Remove 可能持有服务表锁等待网络操作，取消订阅也必须受 ctx 限制。
	done := make(chan error, 1)
	go func() {
		unsubscribed, err := s.unsubscribeAll()
		if err == nil && unsubscribed {
			err = s.flushWithContext(ctx)
		}
		if err == nil {
			// TODO: 关闭流程尚未与 callback 的 wg.Add 同步：Unsubscribe/Flush 后，
			// 已取出的消息仍可能进入 callback，导致 Wait 提前返回或与 Add 发生竞态。
			// 后续需用同一把锁协调 closing 状态与 Add；ErrReplyLater 的延迟回复也未计入等待。
			s.wg.Wait()
			err = s.flushWithContext(ctx)
		}
		done <- err
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-done:
		return err
	}
}

func (s *Server) flushWithContext(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		// NATS FlushWithContext 要求 deadline；无 deadline 时沿用 Flush 的 10 秒上限，
		// 同时保留调用方通过 cancel 取消的能力。
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
	}
	return s.conn.FlushWithContext(ctx)
}

// UnSubscribeAll 取消所有订阅
func (s *Server) UnSubscribeAll() error {
	unsubscribed, err := s.unsubscribeAll()
	if err != nil || !unsubscribed {
		return err
	}
	return s.conn.Flush()
}

// unsubscribeAll 返回是否存在订阅，由调用方选择带 context 或默认超时的 Flush。
func (s *Server) unsubscribeAll() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	unsubs := make([]*nats.Subscription, 0, len(s.services))
	for _, svc := range s.services {
		unsubs = append(unsubs, svc.subscriptions...)
		svc.subscriptions = nil
	}
	var err error
	for _, v := range unsubs {
		if subErr := v.Unsubscribe(); subErr != nil && err == nil {
			err = subErr
		}
	}
	return len(unsubs) > 0, err
}

// Remove 移除一个服务
func (s *Server) Remove(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	svc, ok := s.services[name]
	if ok {
		if len(svc.subscriptions) > 0 {
			for _, subscription := range svc.subscriptions {
				subscription.Unsubscribe()
			}
			s.conn.Flush()
		}
		delete(s.services, name)
	}
	return ok
}

// Register 注册服务
func (s *Server) Register(sd ServiceDesc, val interface{}, opts ...ServiceOption) (ServiceInterface, error) {
	opt := DefaultServiceOptions
	for _, v := range opts {
		v(&opt)
	}

	// new 一个服务
	svc, err := NewService(s, sd, val, opt)
	if nil != err {
		return nil, err
	}

	name := svc.Name()
	s.mu.Lock()
	if _, ok := s.services[name]; ok {
		s.mu.Unlock()
		return nil, ErrDuplicateService
	}

	sw := &serviceWrapper{
		Service: svc,
	}
	err = s.subscribeMethod(sw)
	if err == nil {
		err = s.conn.Flush()
	}
	if err != nil {
		// 注册失败时同步清理所有已创建的订阅，不留下半注册的服务。
		for _, sub := range sw.subscriptions {
			sub.Unsubscribe()
		}
		s.mu.Unlock()
		return nil, err
	}

	s.services[name] = sw
	s.mu.Unlock()

	return svc, nil
}

// subscribeMethod 订阅服务的方法
func (s *Server) subscribeMethod(sw *serviceWrapper) error {
	cb := func(msg *nats.Msg) {
		s.wg.Add(1)
		call := func() {
			defer s.wg.Done()

			method, header, err := decodeHeader(msg.Header)
			if err != nil {
				s.opt.errorHandler(err.Error())
				return
			}
			ctx, cancel := context.WithTimeout(context.Background(), sw.opt.timeout)
			meta := &metaValue{
				header: header,
				reply:  msg.Reply,
				server: s,
				cancel: cancel,
			}
			ctx = withMeta(ctx, meta)

			err = s.handle(ctx, sw, method, msg.Data, msg.Reply)
			if err != nil {
				if errors.Is(err, ErrReplyLater) {
					// reply later
					// 用户自己回复消息
					return
				}
				s.opt.errorHandler(err.Error())
			}
			// 延迟回复由 Reply 在发送成功后取消；未回复时由 timeout 结束。
			cancel()
		}
		if sw.opt.multiGoroutine {
			// TODO 自定义携程池
			go call()
		} else {
			call()
		}
	}

	sub := sw.Name()
	reqSub, subErr := s.conn.QueueSubscribe(sub, defaultQueue, cb)
	if nil != subErr {
		return subErr
	}
	sw.subscriptions = append(sw.subscriptions, reqSub)
	if sw.Service.sd.hasPublishMethod() {
		pubSub, pubErr := s.conn.Subscribe(joinSubject(sub, pubSuffix), cb)
		if pubErr != nil {
			return pubErr
		}
		sw.subscriptions = append(sw.subscriptions, pubSub)
	}
	return nil
}

func (s *Server) handle(ctx context.Context, sw *serviceWrapper, method string, payload []byte, replySub string) error {
	if s.opt.recoverHandler != nil {
		defer func() {
			if e := recover(); e != nil {
				s.opt.recoverHandler(e)
			}
		}()
	}

	b, err := sw.Call(ctx, method, payload, sw.opt.interceptor)

	// publish 不需要回复
	if len(replySub) == 0 {
		return err
	}

	// ErrReplyLater：用户会在别处调用 Reply，这里不回复
	if errors.Is(err, ErrReplyLater) {
		return err
	}

	// 无论成功还是失败都要回复：成功时带上 Data，失败时 Data 为空、
	// 错误信息通过 header 回传，避免客户端只能等待超时且丢失错误。
	// reply header 在成功/失败两种情况下都回传（_ns_error 与 _ns_reply 共存）。
	var replyHeader map[string]string
	if meta := getMeta(ctx); meta != nil {
		replyHeader = meta.snapshotReplyHeader()
	}
	respMsg := &nats.Msg{
		Subject: replySub,
		Header:  addReplyHeader(makeErrorHeader(err), replyHeader),
	}
	if err == nil {
		respMsg.Data = b
	}

	if pubErr := s.conn.PublishMsg(respMsg); pubErr != nil {
		return pubErr
	}
	// 回复已发出，仍把 handler 的业务错误返回给上层用于日志上报
	return err
}

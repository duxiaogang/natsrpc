package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/byebyebruce/natsrpc"
	"github.com/byebyebruce/natsrpc/example"
	"github.com/nats-io/nats.go"
)

var (
	nats_url = flag.String("nats_url", "nats://127.0.0.1:4222", "nats-server地址")
)

func main() {
	conn, err := nats.Connect(*nats_url)
	example.IfNotNilPanic(err)
	defer conn.Close()

	server, err := natsrpc.NewServer(conn)
	example.IfNotNilPanic(err)
	client := natsrpc.NewClient(conn)

	defer server.Close(context.Background())

	svc, err := example.RegisterGreetingNRServer(server, &HelloSvc{})
	example.IfNotNilPanic(err)
	defer svc.Close()

	cli := example.NewGreetingNRClient(client)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// 传入一个 map 指针，请求返回后服务端回传的响应 header 会写入其中。
	var replyHeader map[string]string
	reply, err := cli.Hello(ctx, &example.HelloRequest{
		Name: "bruce",
	}, natsrpc.WithCallReplyHeader(&replyHeader))
	example.IfNotNilPanic(err)

	fmt.Println("reply message:", reply.Message)
	fmt.Println("reply header:", replyHeader)
}

type HelloSvc struct {
}

func (s *HelloSvc) Hello(ctx context.Context, req *example.HelloRequest) (*example.HelloReply, error) {
	// 服务端在 handler 内写响应 header，会随响应回传给客户端。
	// 多次调用按 key 合并，多个中间件也可以各自追加。
	err := natsrpc.SetReplyHeader(ctx, map[string]string{
		"handled-by": "HelloSvc",
		"trace-id":   "abc-123",
	})
	example.IfNotNilPanic(err)

	return &example.HelloReply{
		Message: "hello " + req.Name,
	}, nil
}

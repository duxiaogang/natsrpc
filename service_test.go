package natsrpc

import (
	"context"
	"reflect"
	"testing"
)

func TestMethodDescNewRequest(t *testing.T) {
	for _, tt := range []struct {
		name string
		md   MethodDesc
	}{
		{"legacy descriptor", MethodDesc{RequestType: reflect.TypeOf(serverTestRequest{})}},
		{"factory takes precedence", MethodDesc{
			RequestType:    reflect.TypeOf(serverTestReply{}),
			RequestFactory: func() any { return &serverTestRequest{} },
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			first, ok := tt.md.NewRequest().(*serverTestRequest)
			if !ok {
				t.Fatal("NewRequest() did not return the request type")
			}
			first.Name = "first"
			second := tt.md.NewRequest().(*serverTestRequest)
			if first == second || second.Name != "" {
				t.Fatal("NewRequest() reused a previous request")
			}
		})
	}
}

func TestRequestFactoryPreservesInterceptor(t *testing.T) {
	factoryCalls := 0
	desc := ServiceDesc{
		ServiceName: serverTestGreetingDesc.ServiceName,
		Methods:     append([]MethodDesc(nil), serverTestGreetingDesc.Methods...),
	}
	desc.Methods[0].RequestType = nil
	desc.Methods[0].RequestFactory = func() any {
		factoryCalls++
		return &serverTestRequest{}
	}
	server := &Server{Encoder: defaultEncoder}
	svc, err := NewService(server, desc, &serverTestGreeting{id: "factory"}, DefaultServiceOptions)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := defaultEncoder.Encode(&serverTestRequest{Name: "original"})
	if err != nil {
		t.Fatal(err)
	}
	intercepted := false
	reply, err := svc.Call(context.Background(), "Hello", payload,
		func(ctx context.Context, method string, req interface{}, next Invoker) (interface{}, error) {
			intercepted = true
			typed, ok := req.(*serverTestRequest)
			if !ok || typed.Name != "original" || method != "Hello" {
				t.Fatalf("interceptor received method=%q req=%#v", method, req)
			}
			return next(ctx, &serverTestRequest{Name: "replaced"})
		})
	if err != nil {
		t.Fatal(err)
	}
	var got serverTestReply
	if err := defaultEncoder.Decode(reply, &got); err != nil {
		t.Fatal(err)
	}
	if !intercepted || factoryCalls != 1 || got.Message != "factory:replaced" {
		t.Fatalf("intercepted=%v factoryCalls=%d reply=%q", intercepted, factoryCalls, got.Message)
	}
}

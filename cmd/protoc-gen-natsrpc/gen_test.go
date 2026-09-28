package main

import (
	"flag"
	"strings"
	"testing"

	"github.com/byebyebruce/natsrpc"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

func TestGenerateRequestFactoryPreservesClientAPI(t *testing.T) {
	publish := &descriptorpb.MethodOptions{}
	proto.SetExtension(publish, natsrpc.E_Publish, true)
	file := &descriptorpb.FileDescriptorProto{
		Name:       proto.String("test.proto"),
		Package:    proto.String("test"),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"natsrpc.proto"},
		Options:    &descriptorpb.FileOptions{GoPackage: proto.String("example.com/test;test")},
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("Request")},
			{Name: proto.String("Response")},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{
			Name: proto.String("Greeting"),
			Method: []*descriptorpb.MethodDescriptorProto{
				{Name: proto.String("Hello"), InputType: proto.String(".test.Request"), OutputType: proto.String(".test.Response")},
				{Name: proto.String("Notify"), InputType: proto.String(".test.Request"), OutputType: proto.String(".test.Response"), Options: publish},
			},
		}},
	}
	gen, err := (protogen.Options{ParamFunc: flag.CommandLine.Set}).New(&pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{file.GetName()},
		Parameter:      proto.String("paths=source_relative,omitempty=true"),
		ProtoFile: []*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			protodesc.ToFileDescriptorProto(natsrpc.File_natsrpc_proto),
			file,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	generateFile(gen, gen.FilesByPath[file.GetName()])
	response := gen.Response()
	if response.GetError() != "" {
		t.Fatal(response.GetError())
	}
	if len(response.File) != 1 || response.File[0].GetName() != "test.natsrpc.pb.go" {
		t.Fatalf("unexpected generated files: %v", response.File)
	}
	content := response.File[0].GetContent()
	for _, want := range []string{
		"RequestFactory:",
		"return &Request{}",
		"c.c.Request(ctx,",
		"c.c.Publish(",
		"Notify(notify *Request, opt ...natsrpc.CallOption) error",
		"req.(*Request)",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("generated code does not contain %q", want)
		}
	}
	for _, unwanted := range []string{"reflect.", "RequestType:", "c.c.Invoke(", "go-kratos"} {
		if strings.Contains(content, unwanted) {
			t.Errorf("generated code unexpectedly contains %q", unwanted)
		}
	}
}

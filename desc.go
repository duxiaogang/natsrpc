package natsrpc

import (
	"reflect"
)

// ServiceDesc 服务描述
type ServiceDesc struct {
	ServiceName string       // 服务名
	Methods     []MethodDesc // 方法列表
	Metadata    string       // 元数据
}

func (s ServiceDesc) hasPublishMethod() bool {
	for _, v := range s.Methods {
		if v.IsPublish {
			return true
		}
	}
	return false
}

// MethodDesc 方法描述
type MethodDesc struct {
	MethodName string  // 方法名
	Handler    Handler // 方法处理函数
	IsPublish  bool    // 是否发布
	// RequestType 兼容旧版生成代码。新生成代码使用 RequestFactory。
	RequestType reflect.Type
	// RequestFactory 为每次调用创建独立的请求对象，优先于 RequestType。
	RequestFactory func() any
}

// NewRequest 创建请求
func (md MethodDesc) NewRequest() any {
	if md.RequestFactory != nil {
		return md.RequestFactory()
	}
	return reflect.New(md.RequestType).Interface()
}

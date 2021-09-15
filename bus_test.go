package bus

import (
	"context"
	"testing"
)

func BenchmarkCall(b *testing.B) {
	RegistFunc("test", 3, func(ctx context.Context, args ...interface{}) (interface{}, error) {
		return args[0], nil
	})
	for i := 0; i < b.N; i++ {
		Call("test", "hello")
	}
}

func BenchmarkCallWithContextDirect(b *testing.B) {
	RegistFunc("test", 3, func(ctx context.Context, args ...interface{}) (interface{}, error) {
		return args[0], nil
	})
	for i := 0; i < b.N; i++ {
		CallWithContextDirect(nil, "test", "hello")
	}
}

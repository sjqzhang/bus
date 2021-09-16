package bus

import (
	"context"
	"testing"
	"time"
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

func TestSubscribe(t *testing.T) {
	msgGlobbal := "hello"
	Subscribe("topic", 2, func(ctx context.Context, msg interface{}) {
		if msg.(string) != msgGlobbal {
			t.Fatal()
		}
	})
	Publish("topic", msgGlobbal)
	time.Sleep(time.Second * 1)
}
func TestCall(t *testing.T) {
	argsGlobal1 := "hello"
	argsGlobal2 := 123
	RegistFunc("topic", 2, func(ctx context.Context, args ...interface{}) (interface{}, error) {
	return args,nil
	})
	result,_:=Call("topic", argsGlobal1,argsGlobal2)
	results:=result.([]interface{})
	if results[0].(string)!=argsGlobal1 {
		t.Fatal()
	}
	if results[1].(int)!=argsGlobal2 {
		t.Fatal()
	}
	time.Sleep(time.Second * 1)
}

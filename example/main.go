package main

import (
	"context"
	"fmt"
	bus "github.com/sjqzhang/bus"
)

func Add(a, b int) int {
	return a + b
}

func main() {

	bus.RegistFunc("aa", 3, func(ctx context.Context, args ...interface{}) (interface{}, error) {

		return args[0], nil
	})
	bus.RegistFunc("add", 3, func(ctx context.Context, args ...interface{}) (interface{}, error) {

		return Add(args[0].(int), args[1].(int)), nil
	})
	ctx := context.Background()
	var result interface{}
	var e error
	for i := 0; i < 100; i++ {
		result, _ = bus.Call("aa", "a") //通过channel 调用，相当于异步转同步
		go bus.Call("aa", "a")
		fmt.Println(result)
		result, _ = bus.CallWithContext(ctx, "aa", "b") // 带上下文调用 直接调用
		fmt.Println(result)
		result, _ = bus.CallWithContextDirect(nil, "aa", "c") //直接调用
		fmt.Println(result)
		result, e = bus.CallWithContextDirect(nil, "add", 2, 3)
		fmt.Println(result, e)
	}
	fmt.Println("hello ")
}

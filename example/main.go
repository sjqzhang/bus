package main

import (
	"context"
	"fmt"
	bus "github.com/sjqzhang/bus"
)

func main() {

	bus.SubscribeWithReply("aa", 3, func(ctx context.Context, args ...interface{}) (interface{}, error) {

		return args[0], nil
	})
	ctx:=context.Background()
    var result interface{}
	for i := 0; i < 100; i++ {
		result, _ = bus.Call("aa", "a")             //通过channel 调用，相当于异步转同步
		go bus.Call("aa", "a")
		fmt.Println(result)
		result,_=bus.CallWithContext(ctx, "aa", "b") // 带上下文调用 直接调用
		fmt.Println(result)
		result,_=bus.CallWithContextDirect(nil, "aa", "c") //直接调用
		fmt.Println(result)
	}
	fmt.Println("hello ")
}

package main

import (
	"context"
	"fmt"
	bus "github.com/sjqzhang/bus"
)

func main() {

	bus.SubscribeWithReply("aa",3, func(ctx context.Context, args ...interface{}) (interface{}, error) {


		return args[0],nil
	})

	for i:=0;i<100;i++ {
		ret,_:=bus.Call("aa", "bb")
		fmt.Println(ret)
	}
	fmt.Println("hello ")
}


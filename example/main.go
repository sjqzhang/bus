package main

import (
	"context"
	"fmt"
	bus "github.com/sjqzhang/bus"
)

func main() {

	bus.SubscribeWithReply("aa",3, func(ctx context.Context, msg ...interface{}) (interface{}, error) {

		fmt.Println(msg)
		return "",nil
	})

	for i:=0;i<100;i++ {
		bus.Call("aa", "bb")
	}
	fmt.Println("hello ")
}


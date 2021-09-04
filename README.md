# 消息总线

---

# 获得

`go get -u github.com/sjqzhang/bus`

# 示例

```go
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


```
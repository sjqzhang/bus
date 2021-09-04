# 进程内消息总线
## 为了实现进程内的解藕调用，在进程内实现消息总线，当然因为涉及协程的上下文切换性能存在一定的的损耗，如果要求性能非常高建议使用直接调用。

---

# 如何安装

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



```
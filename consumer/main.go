package main

import (
	"fengqi/beanstalkd/common"
	"fmt"
	"strings"
	"time"
)

func main() {
	conn, err := common.DialBeanstalk()
	if err != nil {
		panic(err)
	}

	for {
		id, body, err := conn.Reserve(time.Second * 10)
		if err != nil {
			fmt.Printf("consume jobs failed: %v\n", err)

			// 这里有可能是确实没消息，也可能是连接断开了需要重连。。。
			if strings.Contains(err.Error(), "connection refused") {
				newConn, err := common.DialBeanstalk()
				if err != nil {
					fmt.Printf("reconnect server failed: %v\n", err)
					time.Sleep(time.Second * 1)
				} else {
					conn = newConn
				}
			}
			continue
		}

		fmt.Printf("consume jobs: %d %s\n", id, body)
		_ = conn.Delete(id)
	}
}

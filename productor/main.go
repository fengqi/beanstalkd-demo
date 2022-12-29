package main

import (
	"fengqi/beanstalkd/common"
	"fmt"
	"time"
)

func main() {
	conn, err := common.DialBeanstalk()
	if err != nil {
		panic(err)
	}

	for {
		// pri 优先级 0优先级最高，取值最大 math.MaxUint32
		// delay 延迟消息，取值最大 math.MaxUint32
		// ttr 最大处理时间，如果consumer从队列取出后指定时间内没有处理完，
		// 也没有重新放回队列或者删除或者延长任务时间都将被自动释放，可以被重新消费
		// ttr最小值为1，传0时将自动+1，最大值 math.MaxUint32
		// https://raw.githubusercontent.com/beanstalkd/beanstalkd/master/doc/protocol.txt
		str := time.Now().String()
		id, err := conn.Put([]byte(str), 1, 0, time.Second*5)
		if err != nil {
			fmt.Printf("produce jobs failed: %v\n", err)

			// 出错可能是链接断了，beanstalk默认并不会自动重连
			newConn, err := common.DialBeanstalk()
			if err != nil {
				fmt.Printf("reconnect server failed: %v\n", err)
				time.Sleep(time.Second * 1)
			} else {
				conn = newConn
			}
			continue
		}

		fmt.Printf("produce jobs: %d %s\n", id, str)
		time.Sleep(time.Second * 3)
	}
}

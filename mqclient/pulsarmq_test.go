/*
 * @Author: yangwenhua
 * @Date: 2023-11-01 17:44:25
 * @LastEditors: yangwenhua
 * @LastEditTime: 2023-11-21 09:18:52
 * @FilePath: \src\mqclient\pulsarmq_test.go
 * @Description:
 *
 *  Copyright 2023 by GETECH  Co., Ltd. All Rights Reserved.
 *  Licensed under the GETECH License,  Version 1.0;
 *  https://www.getech.cn/licenses/LICENSE-1.0
 */
/*
 * @Author: yangwenhua
 * @Date: 2023-09-08 22:09:41
 * @LastEditors: yangwenhua
 * @LastEditTime: 2023-11-03 16:08:21
 * @FilePath: \src\main.go
 * @Description:
 *
 *  Copyright 2023 by GETECH  Co., Ltd. All Rights Reserved.
 *  Licensed under the GETECH License,  Version 1.0;
 *  https://www.getech.cn/licenses/LICENSE-1.0
 */
package mqclient

import (
	_ "context"
	"fmt"

	// "fmt"
	_ "log"
	// "os"
	"testing"
	"time"

	_ "rabbitmq"
)

/**
 * @description: 测试
 * @return {*}
 * @ChangeHistory:
 * date # person # comments
 * -------------------------------------------------------
 * 请手动复制时间 # yangwenhua # initial
 */
func TestMqclient(t *testing.T) {
	var pmq PulsarMQ

	setStr := "{\"URL\":\"pulsar://10.76.65.32:6650\",\"Topic\":\"eas_topic_1\"}"
	pmq.Setting(setStr)
	pmq.Open()
	pmq.SetOption("BusName", "PMQTEST_1")
	pmq.SetChannelOption("Mes", "SendTopic", "eas_topic_1")
	pmq.SetChannelOption("Mes", "SubscriptionName", "Mes_Sub_1")
	pmq.SetChannelOption("Mes", "ListenTopic", "eas_topic_1")
	pmq.SetChannelOption("Mes", "SubscriptionType", "pulsar.Exclusive")

	pmq.EnableChannel("Mes", true)

	pmq.SetChannelOption("ChannelRms", "SendTopic", "eas_topic_2")
	pmq.SetChannelOption("ChannelRms", "ListenTopic", "eas_topic_2")
	// pmq.SetChannelOption("ChannelRms", "SubscriptionName", "rms_sub1")
	pmq.EnableChannel("ChannelRms", true)
	pmq.GetChannelNames()
	pmq.SetOnRecvHandler("Mes", MessageHandle)

	for {
		pmq.Send("Mes", "helloMes")
		pmq.Send("ChannelRms", "Rmshello")
		time.Sleep(2 * 1000 * time.Millisecond)
		// r := pmq.SetOnRecvHandler(msgId+":"+msg, ProcessMessage)
		// pmq.logOnInfo(r.(string))
	}

}

func MessageHandle(msg string) (mo any, err error) {
	mo = msg + "-mo"
	fmt.Printf("Received message -- content: '%s'\n", mo.(string))
	return mo, err
}

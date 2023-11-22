/*
 * @Author: yangwenhua
 * @Date: 2023-11-15 16:50:02
 * @LastEditors: yangwenhua
 * @LastEditTime: 2023-11-22 13:27:24
 * @FilePath: \src\mbcenter\mbc_test.go
 * @Description:
 *
 *  Copyright 2023 by GETECH  Co., Ltd. All Rights Reserved.
 *  Licensed under the GETECH License,  Version 1.0;
 *  https://www.getech.cn/licenses/LICENSE-1.0
 */
package mbcenter

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestMbcenter(t *testing.T) {
	var mbc MBC
	busPulsar1, filePath, setBusStr1 := "pulsar_test", "pulsar", ""
	setJsonStr := "{\"ProtocolConnConfig\":{\"Pulsar\":{\"PulsarProd\":{\"ProtocolOption\":{\"URL\":\"Pulsar://10.133.36.181:6650\",\"ConnectionTimeout\":\"5\",\"OperationTimeout\":\"30\",\"KeepaliveInterval\":\"30\",\"MemoryLimitBytes\":\"64\",\"ConnectionMaxIdleTime\":\"180\"}},\"PulsarLocal\":{\"ProtocolOption\":{\"URL\":\"Pulsar://127.0.0.1:6650\",\"ConnectionTimeout\":\"5\",\"OperationTimeout\":\"30\",\"KeepaliveInterval\":\"30\",\"MemoryLimitBytes\":\"64\",\"ConnectionMaxIdleTime\":\"180\"}},\"PulsarTest\":{\"ProtocolOption\":{\"URL\":\"Pulsar://10.133.36.183:6650\",\"ConnectionTimeout\":\"5\",\"OperationTimeout\":\"30\",\"KeepaliveInterval\":\"30\",\"MemoryLimitBytes\":\"64\",\"ConnectionMaxIdleTime\":\"180\"}}},\"NamedPipe\":{\"NamedPipeTest\":{\"ProtocolOption\":{\"Pipename\":\"FifoTest\",\"IsServer\":\"Y\",\"OpenMode\":\"\"}},\"NamedPipeProd\":{\"ProtocolOption\":{\"PipeName\":\"_Fifo\",\"IsServer\":\"Y\",\"OpenMode\":\"\"}}},\"Http\":{\"HttpTest\":{\"ProtocolOption\":{\"URL\":\"127.0.0.1:8000\",\"ConnectionTimeout\":\"5\",\"KeepaliveInterval\":\"30\",\"SendTimeout\":\"10\"}}}},\"ChannelConfig\":{\"Mes\":{\"BusName\":\"PulsarTest\",\"MsgFormat\":\"Json\",\"ListenTopic\":\"Eas_Mes_Listentopic_1\",\"SendTopic\":\"Eas_Mes_Sendtopic_1\",\"SubscriptionName\":\"Eas_Mes_Subname_1\",\"SendTimeout\":\"10\"},\"Rms\":{\"BusName\":\"PulsarTest\",\"MsgFormat\":\"Json\",\"ListenTopic\":\"Eas_Rms_Listentopic_1\",\"SendTopic\":\"Eas_Rms_Sendtopic_1\",\"SubscriptionName\":\"Eas_Rms_Subname_1\",\"SendTimeout\":\"10\"},\"Fe\":{\"BusName\":\"HttpTest\",\"MsgFormat\":\"Json\",\"SendTimeout\":\"10\"},\"Auto\":{\"BusName\":\"NamedPipeTest\",\"Msgformat\":\"Bson\",\"SendTimeout\":\"10\"}}}"
	config, bus, channel := map[string]interface{}{}, map[string]interface{}{}, map[string]interface{}{}
	json.Unmarshal([]byte(setJsonStr), &config)
	setBusStr1 = "{\"URL\":\"pulsar://10.76.65.32:6650\",\"Topic\":\"eas_topic_1\"}"
	busPulsar2 := "pulsar_test2"
	setBusStr2 := "{\"URL\":\"pulsar://10.76.65.32:6650\",\"SendTopic\":\"eas_topic_2\"}"

	json.Unmarshal([]byte(setBusStr1), &bus)
	setChannelStr := "{\"BusName\":\"PulsarTest\",\"MsgFormat\":\"Json\",\"ListenTopic\":\"Eas_Mes_Listentopic_1\",\"SendTopic\":\"Eas_Mes_Sendtopic_1\",\"SubscriptionName\":\"Eas_Mes_Subname_1\",\"SendTimeout\":\"10\"}"
	json.Unmarshal([]byte(setChannelStr), &channel)
	// setStr = "{\"ProtocolConnConfig\":{\"Pulsar\":{\"PulsarProd\":{\"ProtocolOption\":{\"URL\":\"Pulsar://10.133.36.181:6650\",\"ConnectionTimeout\":\"5\",\"OperationTimeout\":\"30\",\"KeepaliveInterval\":\"30\",\"MemoryLimitBytes\":\"64\",\"ConnectionMaxIdleTime\":\"180\"}},\"PulsarLocal\":{\"ProtocolOption\":{\"URL\":\"Pulsar://127.0.0.1:6650\",\"ConnectionTimeout\":\"5\",\"OperationTimeout\":\"30\",\"KeepaliveInterval\":\"30\",\"MemoryLimitBytes\":\"64\",\"ConnectionMaxIdleTime\":\"180\"}},\"PulsarTest\":{\"ProtocolOption\":{\"URL\":\"Pulsar://10.133.36.183:6650\",\"ConnectionTimeout\":\"5\",\"OperationTimeout\":\"30\",\"KeepaliveInterval\":\"30\",\"MemoryLimitBytes\":\"64\",\"ConnectionMaxIdleTime\":\"180\"}}},\"NamedPipe\":{\"NamedPipeTest\":{\"ProtocolOption\":{\"Pipename\":\"FifoTest\",\"IsServer\":\"Y\",\"OpenMode\":\"\"}},\"NamedPipeProd\":{\"ProtocolOption\":{\"PipeName\":\"_Fifo\",\"IsServer\":\"Y\",\"OpenMode\":\"\"}}},\"Http\":{\"HttpTest\":{\"ProtocolOption\":{\"URL\":\"127.0.0.1:8000\",\"ConnectionTimeout\":\"5\",\"KeepaliveInterval\":\"30\",\"SendTimeout\":\"10\"}}}},\"ChannelConfig\":{\"Mes\":{\"BusName\":\"PulsarTest\",\"MsgFormat\":\"Json\",\"ListenTopic\":\"Eas_Mes_Listentopic_1\",\"SendTopic\":\"Eas_Mes_Sendtopic_1\",\"SubscriptionName\":\"Eas_Mes_Subname_1\",\"SendTimeout\":\"10\"},\"Rms\":{\"BusName\":\"PulsarTest\",\"MsgFormat\":\"Json\",\"ListenTopic\":\"Eas_Rms_Listentopic_1\",\"SendTopic\":\"Eas_Rms_Sendtopic_1\",\"SubscriptionName\":\"Eas_Rms_Subname_1\",\"SendTimeout\":\"10\"},\"Fe\":{\"BusName\":\"HttpTest\",\"MsgFormat\":\"Json\",\"SendTimeout\":\"10\"},\"Auto\":{\"BusName\":\"NamedPipeTest\",\"Msgformat\":\"Bson\",\"SendTimeout\":\"10\"}}}"
	mbc.AddBus(busPulsar1, filePath, setBusStr1)
	mbc.AddBus(busPulsar2, filePath, setBusStr2)
	//1和2相同，1能打开，2也能打开
	retMsg, _ := mbc.Open(busPulsar1)
	// retMsg2, _ := mbc.Open(busPulsar2)
	//打开busPulsar2
	retMsg2, _ := mbc.Open(busPulsar2)

	fmt.Println(retMsg)
	fmt.Println(retMsg2)

	mbc.SetOption(busPulsar1, "BusName", "PMQTEST_1")
	mbc.SetOption(busPulsar1, "Topic", "eas_topic_1")

	mbc.SetChannelOption("Mes", "SendTopic", "eas_topic_1")
	mbc.SetChannelOption("Mes", "SubscriptionName", "Mes_Sub_1")
	mbc.SetChannelOption("Mes", "ListenTopic", "eas_topic_1")
	// mbc.SetChannelOption("Mes", "SubscriptionType", "pulsar.Exclusive")
	mbc.EnableChannel("Mes", true)

	mbc.SetChannelOption("ChannelRms", "SendTopic", "eas_topic_2")
	mbc.SetChannelOption("ChannelRms", "ListenTopic", "eas_topic_2")
	// pmq.SetChannelOption("ChannelRms", "SubscriptionName", "rms_sub1")
	mbc.EnableChannel("ChannelRms", true)

	// mbc.SetOption("key", "value")
	// mbc.Open(busPulsar)
	// mbc.Close(busPulsar)
	// mbc.EnableChannel("channelName", true)
	// mbc.SetChannelOption("channelName", "key", "value")
}

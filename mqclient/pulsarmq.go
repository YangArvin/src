/*
 * @Author: yangwenhua
 * @Date: 2023-11-01 09:18:27
 * @LastEditors: yangwenhua
 * @LastEditTime: 2023-11-17 13:33:14
 * @FilePath: \src\mqclient\pulsarmq.go
 * @Description:
 *
 *  Copyright 2023 by GETECH  Co., Ltd. All Rights Reserved.
 *  Licensed under the GETECH License,  Version 1.0;
 *  https://www.getech.cn/licenses/LICENSE-1.0
 */
/*
 * @Author: yangwenhua
 * @Date: 2023-11-01 09:18:27
 * @LastEditors: yangwenhua
 * @LastEditTime: 2023-11-15 18:04:36
 * @FilePath: \src\mqclient\mqclient.go
 * @Description:
 *
 *  Copyright 2023 by GETECH  Co., Ltd. All Rights Reserved.
 *  Licensed under the GETECH License,  Version 1.0;
 *  https://www.getech.cn/licenses/LICENSE-1.0
 */

// go:binary-only-package

package mqclient

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

// PulsarMQ结构体
type PulsarMQ struct {
	// pulsar客户端
	BusClient pulsar.Client
	// pulsar ClientOption
	ClientOption pulsar.ClientOptions
	// channels
	ChannelMap map[string]*Channel
	//BusName
	BusName string
}

// 初始化配置
func (p *PulsarMQ) Setting(settingJson string) (retMsg string, err error) {
	retMsg = "解析Json配置文件成功"
	// Creating the maps for JSON
	m := map[string]interface{}{}
	err = json.Unmarshal([]byte(settingJson), &m)
	if err != nil {
		retMsg = "解析Json配置文件失败"
		p.logOnErr(err, retMsg)
		return retMsg, err
	}
	//Json转Option
	parseMapToOption(p, m)
	p.logOnErr(err, retMsg)
	return retMsg, err
}

// 是否端对端协议
func (p *PulsarMQ) IsSingle() bool {
	//判断标准
	return false
}

// 打开一个pulsarClient
func (p *PulsarMQ) Open() (retMsg string, err error) {
	sb := strings.Builder{}
	sb.WriteString("连接pulsarClient")
	sb.WriteString(p.ClientOption.URL)
	sb.WriteString("成功~")
	retMsg = sb.String()

	client, err := pulsar.NewClient(p.ClientOption)
	if err != nil {
		retMsg = "连接pulsar" + p.ClientOption.URL + "失败"
	}
	p.BusClient = client
	p.logOnErr(err, retMsg)
	return retMsg, err
}

// 关闭一个pulsarClient
func (p *PulsarMQ) Close() {
	p.BusClient.Close()
}

/**
 * @description: 设置全部Option参数
 * @return {*}
 * @ChangeHistory:
 * date # person # comments
 * -------------------------------------------------------
 * 请手动复制时间 # yangwenhua # initial
 */
func (p *PulsarMQ) SetOption(key string, value any) (retMsg string, err error) {
	retMsg = key + "：在ClientOption中存在，设置成功！"
	switch key {
	case "BusName":
		p.BusName = value.(string)
	case "URL":
		p.ClientOption.URL = value.(string)
	case "ConnectionTimeout":
		p.ClientOption.ConnectionTimeout = value.(time.Duration)
	case "OperationTimeout":
		p.ClientOption.OperationTimeout = value.(time.Duration)
	case "KeepaliveInterval":
		p.ClientOption.KeepAliveInterval = value.(time.Duration)
	case "MemoryLimitBytes":
		p.ClientOption.MemoryLimitBytes = value.(int64)
	case "ConnectionMaxIdleTime":
		p.ClientOption.ConnectionMaxIdleTime = value.(time.Duration)
	default:
		retMsg = key + "：在ClientOption中不存在!"
	}
	p.logOnErr(err, retMsg)
	return retMsg, nil
}

// 设置channel参数
func (p *PulsarMQ) SetChannelOption(channelName string, key string, value any) (retMsg string, err error) {
	channelMap := p.ChannelMap
	//new一个channel,默认消费类型为pulsar.Shared，Exclusive不支持多开comsumer
	newChannel := &Channel{}
	newChannel.SubscriptionType = pulsar.Shared
	if channelMap == nil {
		//判断Map是否为空，是就添加，不是就更新参数
		channelMap = make(map[string]*Channel)
		newChannel.setChannelKey(key, value)
		channelMap[channelName] = newChannel
		p.ChannelMap = channelMap
		retMsg = key + "：在ChannelOption中设置成功，已为ChannelMap添加" + channelName + "!"
	} else {
		//判断是否端对端，是只能有一个channel
		if p.IsSingle() {
			if len(channelMap) == 1 {
				channelMap[channelName].setChannelKey(key, value)
				retMsg = key + "：在ChannelOption中设置成功，已为ChannelMap更新,Channel为Single Bus" + channelName + "!"
			}
		} else {
			//不是可以有多个channel，没有就添加channel,有就更新channel
			if _, ok := channelMap[channelName]; !ok {
				newChannel.setChannelKey(key, value)
				channelMap[channelName] = newChannel
				p.ChannelMap = channelMap
				retMsg = key + "：在ChannelOption中设置成功，已为ChannelMap添加" + channelName + "!"
			} else {
				channelMap[channelName].setChannelKey(key, value)
				retMsg = key + "：在ChannelOption中设置成功，已为ChannelMap更新" + channelName + "!"
			}
		}
	}
	p.logOnErr(err, retMsg)
	return retMsg, err
}

// 启动通道
func (p *PulsarMQ) EnableChannel(channelName string, enableFlag bool) (retMsg string, err error) {
	if p.BusClient == nil {
		retMsg = " ChannelName: " + channelName + "所在Bus: " + p.BusName + " 未完成BusClient初始化，无法操作Channel"
		p.logOnErr(err, retMsg)
		return retMsg, err
	}
	channelMap := p.ChannelMap
	rcvChannel := make(chan pulsar.ConsumerMessage, 100)

	if channelMap == nil {
		//判断Map是否为空，是就添加，不是就更新参数
		retMsg = " ChannelMap为空，无法操作Channel！"
		p.logOnErr(err, retMsg)
		return retMsg, err
	} else {
		//判断Map中有没有channelName的channel，没有就返回，有就操作Channel
		for k, v := range channelMap {
			if k == channelName && v != nil {
				//最终确定channelType
				v.setChannelType(channelName)
				//开关Channel
				if enableFlag {
					retMsg = "ChannelName: " + channelName + " 启动 Channel成功"
					//根据channelType启动Producer或Consumer,此处会更新channel
					switch v.ChannelType {
					case "ProducerAndConsumer":
						//默认consumer为shared
						v.ConsumerOptions.Type = v.SubscriptionType
						//创建producer和Consumer
						v.ConsumerOptions.Topic = v.ListenTopic
						v.ConsumerOptions.SubscriptionName = v.SubscriptionName
						v.ConsumerOptions.MessageChannel = rcvChannel
						v.ProducerOptions.Topic = v.SendTopic
						if v.SendTimeOut != 0 {
							v.ProducerOptions.SendTimeout = time.Duration(v.SendTimeOut) * time.Second
						}
						producer, err := p.BusClient.CreateProducer(v.ProducerOptions)
						if err != nil {
							retMsg = "ChannelName: " + channelName + " 创建producer失败"
							p.logOnErr(err, retMsg)
							return retMsg, err
						}
						v.Producer = producer
						consumer, err := p.BusClient.Subscribe(v.ConsumerOptions)
						if err != nil {
							retMsg = "ChannelName: " + channelName + " 创建consumer失败"
							p.logOnErr(err, retMsg)
							return retMsg, err
						}
						v.Consumer = consumer
					case "Producer":
						//创建producer
						v.ProducerOptions.Topic = v.SendTopic
						if v.SendTimeOut != 0 {
							v.ProducerOptions.SendTimeout = time.Duration(v.SendTimeOut) * time.Second
						}
						producer, err := p.BusClient.CreateProducer(v.ProducerOptions)
						if err != nil {
							retMsg = "ChannelName: " + channelName + " 创建producer失败"
							p.logOnErr(err, retMsg)
							return retMsg, err
						}
						v.Producer = producer
					case "Consumer":
						//默认consumer为shared
						v.ConsumerOptions.Type = v.SubscriptionType
						//创建Consumer
						v.ConsumerOptions.Topic = v.ListenTopic
						v.ConsumerOptions.SubscriptionName = v.SubscriptionName
						v.ConsumerOptions.MessageChannel = rcvChannel
						consumer, err := p.BusClient.Subscribe(v.ConsumerOptions)
						if err != nil {
							retMsg = "ChannelName: " + channelName + " 创建consumer失败"
							p.logOnErr(err, retMsg)
							return retMsg, err
						}
						v.Consumer = consumer
					case "None":
						//无法创建
						retMsg = "ChannelName: " + channelName + " :无法创建"
						p.logOnErr(err, retMsg)
						return retMsg, err
					default:
						break
					}
				} else {
					//关闭Channel
					if v.Producer != nil {
						v.Producer.Close()
					}
					if v.Consumer != nil {
						v.Consumer.Close()
					}
					retMsg = " ChannelName: " + channelName + " 关闭 Channel成功"
				}
			}
		}
	}
	p.logOnErr(err, retMsg)
	return retMsg, err
}

// 获取ChannelName 返回切片
func (p *PulsarMQ) GetChannelNames() ([]string, error) {
	var nameSlice []string
	var err error
	channelMap := p.ChannelMap
	if channelMap == nil {
		//判断Map是否为空，空就返回空切片
		p.logOnErr(err, " channelMap为空")
		return nameSlice, err
	} else {
		//将channelName返回到切片
		for k := range channelMap {
			nameSlice = append(nameSlice, k)
		}
	}
	p.logOnErr(err, " ChannelNames: "+strings.Join(nameSlice, ","))
	return nameSlice, err
}

// 发送消息
func (p *PulsarMQ) Send(channelName string, msgStr string) (retMsg string, err error) {
	channelSlice, err := p.GetChannelNames()
	if err != nil {
		retMsg = " 获取ChannelName: " + channelName + " 失败!"
		p.logOnErr(err, retMsg)
		return retMsg, err
	}
	for _, channel := range channelSlice {
		if channel == channelName {
			channelObj := p.ChannelMap[channel]
			if channelObj == nil {
				//channelName所指channel为空
				retMsg = " ChannelName:" + channelName + ":所指channel为空!"
				p.logOnErr(err, retMsg)
				return retMsg, err
			}
			msg := &pulsar.ProducerMessage{
				Payload: []byte(msgStr),
			}
			if channelObj.Producer != nil {
				msgId, err := channelObj.Producer.Send(context.Background(), msg)
				p.logSendMsgRet(retMsg, err, msgStr, msgId.String())
				retMsg = " ChannelName: " + channelName + " :发送消息成功!"
			} else {
				retMsg = " ChannelName: " + channelName + " :所指Producer为空!"
				p.logOnErr(err, retMsg)
			}
		}
	}
	p.logOnErr(err, retMsg)
	return retMsg, err
}

// 接收消息
func (p *PulsarMQ) OnRecvOne(channelName string) (retMsg string, msgStr string, msgId string, err error) {
	channelSlice, err := p.GetChannelNames()
	if err != nil {
		retMsg = " 获取ChannelNames失败!"
		p.logOnErr(err, retMsg)
		return retMsg, "", "", err
	}
	for _, channel := range channelSlice {
		if channel == channelName {
			channelObj := p.ChannelMap[channel]
			if channelObj == nil {
				//channelName所指channel为空
				retMsg = "ChannelName: " + channelName + " 所指channel为空!"
				p.logOnErr(err, retMsg)
				return retMsg, "", "", err
			}
			if channelObj.Consumer == nil {
				retMsg = "ChannelName: " + channelName + " 所指Consumer为空!"
				p.logOnErr(err, retMsg)
				return retMsg, "", "", err
			}
			msg, err := channelObj.Consumer.Receive(context.Background())
			if err != nil {
				retMsg = "ChannelName: " + channelName + " 接收消息失败!"
				p.logOnErr(err, retMsg)
			}
			if msg != nil {
				msgStr = string(msg.Payload())
				msgId = msg.ID().String()
				channelObj.Consumer.Ack(msg)
				p.logReceiveMsgRet(retMsg, err, msgStr, msgId)
			} else {
				channelObj.Consumer.Nack(msg)
			}
		}
	}
	return retMsg, msgStr, msgId, err
}

// 接收消息回调函数
func (p *PulsarMQ) SetOnRecvHandler(channelName string, handler func(string) (any, error)) error {
	channelSlice, err := p.GetChannelNames()
	if err != nil {
		return err
	}
	for _, channel := range channelSlice {
		if channel == channelName {
			channelObj := p.ChannelMap[channel]
			if channelObj == nil {
				//channelName所指channel为空
				return err
			}
			if channelObj.Consumer == nil {
				return err
			}

			go func() {
				for cm := range channelObj.ConsumerOptions.MessageChannel {
					msg := cm.Message
					if msg != nil {
						msgStr := string(msg.Payload())
						handler(msgStr)
						channelObj.Consumer.Ack(msg)
					} else {
						channelObj.Consumer.Nack(msg)
					}
				}
			}()
		}
	}
	return err
}

// channe结构体
type Channel struct {
	// ChannelType
	ChannelType string
	// BusName
	BusName string
	// ChannelName
	ChannelName string
	// producerTopicName
	SendTopic string
	// consumerTopicName
	ListenTopic string
	// sendTimeOut
	SendTimeOut int
	// msgFormat
	MsgFormat string
	// SubscriptionName
	SubscriptionName string
	// SubscriptionType
	SubscriptionType pulsar.SubscriptionType
	// MsgTxidPath
	MsgTxidPath string
	// MsgNamePath
	MsgNamePath string
	// pulsar生产者
	Producer pulsar.Producer
	// pulsar消费者
	Consumer pulsar.Consumer
	// pulsar ProducerOptions
	ProducerOptions pulsar.ProducerOptions
	// pulsar ConsumerOptions
	ConsumerOptions pulsar.ConsumerOptions
}

func (c *Channel) setChannelKey(key string, value any) (retMsg string, err error) {
	//判断Map是否为空，是就添加，有就更新参数
	switch key {
	case "ChannelType":
		c.ChannelType = value.(string)
	case "BusName":
		c.BusName = value.(string)
	case "MsgFormat":
		c.MsgFormat = value.(string)
	case "SendTimeOut":
		c.SendTimeOut = value.(int)
	case "SubscriptionName":
		c.SubscriptionName = value.(string)
	case "SubscriptionType":
		if strings.Contains(value.(string), "Shared") {
			c.SubscriptionType = pulsar.Shared
		}
		if strings.Contains(value.(string), "Exclusive") {
			c.SubscriptionType = pulsar.Exclusive
		}
		if strings.Contains(value.(string), "Failover") {
			c.SubscriptionType = pulsar.Failover
		}
		if strings.Contains(value.(string), "KeyShared") {
			c.SubscriptionType = pulsar.KeyShared
		}
	case "SendTopic":
		c.SendTopic = value.(string)
	case "MsgTxidPath":
		c.MsgTxidPath = value.(string)
	case "MsgNamePath":
		c.MsgNamePath = value.(string)
	case "ListenTopic":
		c.ListenTopic = value.(string)
	default:
		retMsg = key + "：在ChannelOption中不存在!"
	}
	retMsg = key + "：在ChannelOption中设置完毕!"
	return retMsg, err
}

func (c *Channel) setChannelType(channelName string) (retMsg string, err error) {
	//判断channel参数决定channelType
	if c.SendTopic != "" && c.ListenTopic != "" && c.SubscriptionName != "" {
		c.ChannelType = "ProducerAndConsumer"
	}
	if c.SendTopic != "" && c.ListenTopic == "" && c.SubscriptionName != "" {
		c.ChannelType = "Producer"
	}
	if c.SendTopic != "" && c.ListenTopic != "" && c.SubscriptionName == "" {
		c.ChannelType = "Producer"
	}
	if c.SendTopic == "" && c.ListenTopic != "" && c.SubscriptionName != "" {
		c.ChannelType = "Consumer"
	}
	if c.SendTopic == "" && c.ListenTopic == "" {
		c.ChannelType = "None"
	}
	retMsg = "当前设置Channel为" + c.ChannelType + "!"
	return retMsg, err
}

// 错误处理函数
func (r *PulsarMQ) logOnErr(err error, message string) {
	if err != nil {
		log.Printf("%s:%s", message, err)
		panic(fmt.Sprintf("%s:%s", message, err))
	} else {
		log.Printf("%s:%s", message, "OK")
	}
}

// Info处理函数
func (r *PulsarMQ) logOnInfo(message string) {
	log.Printf("%s", message)
}

// 发送消息返回日志
func (r *PulsarMQ) logSendMsgRet(retMsg string, err error, message string, msgId string) {
	if err != nil {
		log.Printf("%s:%s:%s:%s", retMsg, msgId, "NG", err)
	} else {
		log.Printf("Send: %s:%s:%s:%s", retMsg, msgId, message, "")
	}
}

// 接收消息返回日志
func (r *PulsarMQ) logReceiveMsgRet(retMsg string, err error, message string, msgId string) {
	if err != nil {
		log.Printf("%s:%s:%s:%s", retMsg, msgId, "NG", err)
		panic(fmt.Sprintf("%s:%s:%s:%s", retMsg, msgId, message, err))
	} else {
		log.Printf("Receive: %s, msgId: %s, message: %s", retMsg, msgId, message)
	}
}

func Try(fun func(), handler func(interface{})) {
	defer func() {
		if err := recover(); err != nil {
			handler(err)
		}
	}()
	fun()
}

/**
 * @description:从setJson遍历Map转Option存放
 * @param {*PulsarMQ} p
 * @param {map[string]interface{}} aMap
 * @return {*}
 * @ChangeHistory:
 * date # person # comments
 * -------------------------------------------------------
 * 请手动复制时间 # yangwenhua # initial
 */
func parseMapToOption(p *PulsarMQ, aMap map[string]interface{}) {
	for key, val := range aMap {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			parseMapToOption(p, val.(map[string]interface{}))
		case []interface{}:
			parseArray(p, val.([]interface{}))
		case string:
			p.SetOption(key, val)
		default:
			fmt.Println(key, ":", concreteVal)
		}
	}
}

/**
 * @description:遍历数组
 * @param {*PulsarMQ} p
 * @param {[]interface{}} anArray
 * @return {*}
 * @ChangeHistory:
 * date # person # comments
 * -------------------------------------------------------
 * 请手动复制时间 # yangwenhua # initial
 */
func parseArray(p *PulsarMQ, anArray []interface{}) {
	for i, val := range anArray {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			fmt.Println("Index:", i)
			parseMapToOption(p, val.(map[string]interface{}))
		case []interface{}:
			fmt.Println("Index:", i)
			parseArray(p, val.([]interface{}))
		default:
			fmt.Println("Index", i, ":", concreteVal)
		}
	}
}

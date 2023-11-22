package mbcenter

import (
	"fmt"
	"strings"

	"mqclient"
)

// // 协议的类型，怎么添加新的？TODO
// type PulsarMQ mqclient.PulsarMQ
// type RabbitMQ mqclient.RabbitMQ

// type mqcacheable interface {
// 	PulsarMQ | RabbitMQ
// }

// type msgBusTs[T mqcacheable] struct {
// 	BusTypeMap map[string]T
// }

// func (mbTs *msgBusTs[T]) Set(key string, value T) {
// 	mbTs.BusTypeMap[key] = value
// }

// func (mbTs *msgBusTs[T]) Get(key string) (v T) {
// 	if v, ok := mbTs.BusTypeMap[key]; ok {
// 		return v
// 	}
// 	return
// }

// func New[T mqcacheable]() *msgBusTs[T] {
// 	mbc := msgBusTs[T]{}
// 	mbc.BusTypeMap = make(map[string]T)
// 	return &mbc
// }

// type Map[K string, V mqcacheable] map[K]V

type stdBusAPI interface {
	Setting(settingJson string) (retMsg string, err error)
	IsSingle() bool
	Open() (retMsg string, err error)
	Close()
	SetOption(key string, value any) (retMsg string, err error)
	SetChannelOption(channelName string, key string, value any) (retMsg string, err error)
	EnableChannel(channelName string, enableFlag bool) (retMsg string, err error)
	GetChannelNames() ([]string, error)
	Send(channelName string, msgStr string) (retMsg string, err error)
	OnRecvOne(channelName string) (retMsg string, msgStr string, msgId string, err error)
	SetOnRecvHandler(channelName string, handler func(string) (any, error)) error
}

// MBC 结构体
type MBC struct {
	//MbcName
	MbcName string
	//JsonConfigStr
	JsonConfigStr string
	// 泛型Bus实例 需要能放各种Bus先放PulsarMQ
	BusMap map[string]stdBusAPI
	// ChannelMap
	ChannelMap map[string]*mqclient.Channel
}

// // New Center
// func New[T mqcacheable]() *MBC {
// 	mbc := MBC[T]{}
// 	mbc.BusMap = make(map[string]T)
// 	return &mbc
// }

// // 设置bus
// func (mbc *MBC) Set(key string, value T) {
// 	mbc.BusMap[key] = value
// }

// // 获取bus
// func (mbc *MBC) Get(key string) (v T) {
// 	if v, ok := mbc.BusMap[key]; ok {
// 		return v
// 	}
// 	return
// }

// // New Center
// func New[T mqcacheable]() *MBC {
// 	mbc := MBC[T]{}
// 	mbc.BusMap = make(map[string]T)
// 	return &mbc
// }

// 判断busType
func (mbc *MBC) ImportBus(filePath string) (busType string) {
	// TODO
	if strings.Contains(strings.ToLower(filePath), "pulsar") {
		busType = "pulsar"
	}
	//判断busType
	return busType
}

// 添加bus
func (mbc *MBC) AddBus(busName string, filePath string, settingStr string) (retMsg string, err error) {
	//判断busType
	busType := mbc.ImportBus(filePath)
	switch busType {
	case "pulsar":
		var pmq mqclient.PulsarMQ
		var stdBus stdBusAPI
		//实现接口
		stdBus = &pmq
		stdBus.Setting(settingStr)
		stdBus.SetOption("BusName", busName)
		busMap := mbc.BusMap
		if busMap == nil {
			busMap = make(map[string]stdBusAPI)
			busMap[busName] = stdBus
			mbc.BusMap = busMap
		} else {
			if _, ok := busMap[busName]; !ok {
				busMap[busName] = stdBus
				mbc.BusMap = busMap
				retMsg = "已添加busName:" + busName + "!"
			} else {
				retMsg = "已存在busName:" + busName + "!"
			}
		}
	default:
		retMsg = "busName:" + busName + "不存在!"
		err = fmt.Errorf("busName:" + busName + "不存在!")
	}
	return retMsg, err
}

// 打开一个bus
func (mbc *MBC) Open(busName string) (retMsg string, err error) {
	if _, ok := mbc.BusMap[busName]; !ok {
		retMsg = "MBC中无" + busName + "!,请添加!"
	} else {
		v, ok := mbc.BusMap[busName]
		if ok {
			retMsg, err = v.Open()
		}
	}
	return retMsg, err
}

// 关闭一个bus
func (mbc *MBC) Close(busName string) (retMsg string, err error) {
	if _, ok := mbc.BusMap[busName]; !ok {
		retMsg = "MBC中无" + busName + "!,请添加!"
	} else {
		v, ok := mbc.BusMap[busName]
		if ok {
			v.Close()
		}
	}
	return retMsg, err
}

/**
 * @description: 设置BusOption参数
 * @return {*}
 * @ChangeHistory:
 * date # person # comments
 * -------------------------------------------------------
 * 请手动复制时间 # yangwenhua # initial
 */
func (mbc *MBC) SetOption(busName string, key string, value any) (retMsg string, err error) {
	//不是可以有多个channel，没有就添加channel,有就更新channel
	if b, ok := mbc.BusMap[busName]; !ok {
		retMsg = busName + "：不存在!"
	} else {
		b.SetOption(key, value)
		retMsg = key + "：在Option中设置成功，已为BusMap更新" + busName + "!"
	}
	return retMsg, err
}

// 设置channel参数
func (mbc *MBC) SetChannelOption(channelName string, key string, value any) (retMsg string, err error) {
	//没有busName情况下循环会在Map下新建全部的channel
	for _, bus := range mbc.BusMap {
		retMsg, err = bus.SetChannelOption(channelName, key, value)
	}
	return retMsg, err
}

// 启动通道
func (mbc *MBC) EnableChannel(channelName string, enableFlag bool) (retMsg string, err error) {
	//没有busName情况下循环会把Map中有的channelName全部打开
	for _, bus := range mbc.BusMap {
		retMsg, err = bus.EnableChannel(channelName, enableFlag)
	}
	return retMsg, err
}

// 发送SendPrimary消息
func (mbc *MBC) SendPrimary(channelName string) (retMsg string, err error) {

	return retMsg, err
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
func parseMapToOption(mbc *MBC, aMap map[string]interface{}) {
	for key, val := range aMap {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			parseMapToOption(mbc, val.(map[string]interface{}))
		case []interface{}:
			parseArray(mbc, val.([]interface{}))
		case string:
			//TODO
			mbc.SetOption("???", key, val)
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
func parseArray(mbc *MBC, anArray []interface{}) {
	for i, val := range anArray {
		switch concreteVal := val.(type) {
		case map[string]interface{}:
			fmt.Println("Index:", i)
			parseMapToOption(mbc, val.(map[string]interface{}))
		case []interface{}:
			fmt.Println("Index:", i)
			parseArray(mbc, val.([]interface{}))
		default:
			fmt.Println("Index", i, ":", concreteVal)
		}
	}
}

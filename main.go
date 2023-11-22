/*
 * @Author: yangwenhua
 * @Date: 2023-11-01 17:44:25
 * @LastEditors: yangwenhua
 * @LastEditTime: 2023-11-17 16:44:44
 * @FilePath: \src\main.go
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
package main

import (
	_ "context"
	"fmt"
	_ "log"
	_ "net/http"
	"os"
	_ "plugin"
	_ "time"

	_ "mqclient"
	_ "rabbitmq"

	_ "github.com/apache/pulsar-client-go/pulsar"
	"github.com/traefik/yaegi/interp"
	"github.com/traefik/yaegi/stdlib"
	"gopkg.in/ini.v1"
)

/**
 * @description:
 * @return {*}
 * @ChangeHistory:
 * date # person # comments
 * -------------------------------------------------------
 * 请手动复制时间 # yangwenhua # initial
 */
func main() {

	type mydata[T any] []T

	var a mydata[int] = []int{1}
	var b mydata[string] = []string{"123"}
	type myTransaction[T comparable] struct {
	}

	fmt.Println(a)
	fmt.Println(b)

	cfg, err := ini.Load("my.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v", err)
		os.Exit(1)
	}

	// 典型读取操作，默认分区可以使用空字符串表示
	fmt.Println("App Mode:", cfg.Section("").Key("app_mode").String())
	fmt.Println("Data Path:", cfg.Section("paths").Key("data").String())

	// 我们可以做一些候选值限制的操作
	fmt.Println("Server Protocol:",
		cfg.Section("server").Key("protocol").In("http", []string{"http", "https"}))
	// 如果读取的值不在候选列表内，则会回退使用提供的默认值
	fmt.Println("Email Protocol:",
		cfg.Section("server").Key("protocol").In("smtp", []string{"imap", "smtp"}))

	// 试一试自动类型转换
	fmt.Printf("Port Number: (%[1]T) %[1]d\n", cfg.Section("server").Key("http_port").MustInt(9999))
	fmt.Printf("Enforce Domain: (%[1]T) %[1]v\n", cfg.Section("server").Key("enforce_domain").MustBool(false))

	// 差不多了，修改某个值然后进行保存
	cfg.Section("").Key("app_mode").SetValue("production")
	cfg.SaveTo("my.ini.local")

	// testMsg := "hello"
	// testUrl := "pulsar://10.133.36.183:8080"
	// testTopic := "eas_topic_1"
	// 	// publish message

	// var pmq mqclient.PulsarMQ

	// pmq.Setting(testUrl)

	// 初始化解释器
	i := interp.New(interp.Options{GoPath: "./idea_code/go/"})

	// 插件需要使用标准库
	if err := i.Use(stdlib.Symbols); err != nil {
		panic(err)
	}

	// 导入 hello 包
	if _, err := i.Eval(`import "mqclient.pulsarmq"`); err != nil {
		panic(err)
	}

	// 调用 hello.CallMe
	v, err := i.Eval("Pulsar.Setting")
	if err != nil {
		panic(err)
	}
	Setting := v.Interface().(func(string) string)
	setStr := "{\"URL\":\"pulsar://10.76.65.32:6650\",\"Topic\":\"eas_topic_1\"}"
	fmt.Println(Setting(setStr))
}

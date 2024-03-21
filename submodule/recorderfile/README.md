# Recorderfile

用于记录数据到文件

# 用法

## 数据定义

在 recorderfile.go 中声明需要记录到文件的表名和表头，然后在 start 函数中初始化
中引入 recorderfile 包，并定义需要记录到数据库中的数据结构原型，然后在 `init()` 函数中使用 `recorderfile.RegisterModel()` 函数注册数据模型：

```go
func TransactionPoolInputThroughputInit() {
	path := fmt.Sprintf("%s/transaction_pool_input_throughput.csv", Workdir)
	TransactionPoolInputThroughputF, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println(TransactionPoolInputThroughputF, "transaction_pool_input_throughput open failed!")
	}
	defer TransactionPoolInputThroughputF.Close()

	str := "measure_time,tx_id,source\n" //表头

	//写入表头，传入数据为切片(追加模式)
	_, err1 := TransactionPoolInputThroughputF.WriteString(str)
	if err1 != nil {
		log.Println("[transaction_pool_input_throughput] init failed")
	}
	log.Println("[transaction_pool_input_throughput] init succeed")
}


func ConfigInit() {
	TransactionPoolInputThroughputInit()
	......
}

func Start(port uint16) error {
	ConfigInit()
        ......
	return nil
}

```

## 记录数据

在需要记录数据的地方，构造数据 Object，然后使用 `recorderfile.Record()` 函数将数据记录到对应文件，`recorderfile.Record()` 函数是异步执行的。

```go
package some_package

import (
  "chainmaker.org/chainmaker/recorderfile"
  ......
)

func (pool *normalPool) OnMessage(msg *msgbus.Message) {
  ......
  for _, tx := range txs {
    str := fmt.Sprintf("%s,%s,%s\n", time.Now().Format("2006-01-02 15:04:05.000000"), tx.Payload.GetTxId(), strconv.Itoa(int(1))) //需要写入csv的数据，切片类型
    strs = strs + str
  }
  _ = recorderfile.Record(strs, "transaction_pool_input_throughput")
  ......
}
```

## 连接数据库

在主程序启动阶段，使用 `recorderfile.Start(port)` 函数初始化性能指标文件及表头
参数解释：

- `port`  
  `recorderfile` 框架启动后，会在后台启动一个 `http server`, 并倾听该 `port` 端口，用于动态更新 `configuration`，下文会详细介绍。如果该 `port` 值为 `0`，那么 `http server` 会默认倾听 `9527` 端口。

```go
package main

import (
  "chainmaker.org/chainmaker/recorderfile"
  ......
)

func main() {
  ......
  recorderfile.Start(port)
  ......
}
```

上述介绍的 `recorderfile` 函数，需要确保其执行顺序为：

```go
recorderfile.Start() // 在程序启动过程中执行

recorderfile.Record() // 在程序启动后的业务函数中执行
```

# 动态更新 configuration

项目启动后，`recorderfile` 框架会启动一个基于 `HTTP` 协议的 `configServer`， 该 `configServer` 倾听的端口即为 `recorderfile.Start(dns, port)` 传入的 `port` 参数，如果参数 `port` 为 `0`, `configServer` 会默认倾听 `9527` 端口。  
参考本框架内的 `Makefile` 文件，可以方便地使用 `make someCommand` 命令向 `configServer` 发送 `request`。


## 更新 `recorderfile.Record()` 允许记录的 model

```bash
$ make updateaccessconfig
ALL: true
SampleStruct: true
SampleTransaction: false

# 此时再查看 accessconfig，返回的结果就不是 {} 了
$ make accessconfig
ALL: true
SampleStruct: true
SampleTransaction: false
```

`make updateaccessconfig` 命令读取的是 `Makefile` 同目录下的 `accessconfig.yml` 文件并传送给 `configServer` 的，该文件格式如下:

```bash
$ cat accessconfig.yml
ALL: true
SampleTransaction: false
SampleStruct: true
```

以 `SampleTransaction` 为例，只有在 `ALL` 为 `true` 且 `SampleTransaction` 为 `true` 的情况下，才允许 `recorderfile.Record()` 记录 `SampleTransaction`。  
如果 `ALL` 为 `false`，则不允许记录任何数据。

# `recorderfile.GetConfigValue(key)` 的使用

在业务逻辑中，可能需要读取一些动态配置的参数，可以使用 `recorderfile.GetConfigValue(key)` 读取配置数据：

```go
package some_package

import (
  "chainmaker.org/chainmaker/recorderfile"
  ......
)

func (cal *Calculator) func DoScale(total int) int {
	scale := 1.0
	if scaleVal, ok := recorderfile.GetConfigValue("scale"); ok {
		if val, ok := scaleVal.(float64); ok {
			if val != 0 { // 如果是0，可以当做没有该项配置
				scale = val
			}
		}
	}
	return int(float64(total) * scale)
}
```

## 查看 `recorderfile.GetConfigValue(key)` 可以获取到的值

```bash
$ make configvalue
{}
```

初始情况下，`configValue` 为空，需要通过 `make updateconfigvalue` 命令上传允许 `recorderfile.GetConfigValue(key)` 获取的值

## 更新 `recorderfile.GetConfigValue(key)` 可以获取到的值

```bash
$ make updateconfigvalue
rate: 100
scale: 12.2
```

此时读取 `configVallue` 的值，就不为空了:

```bash
$ make configvalue
rate: 100
scale: 12.2
```

`make updateconfigvalue` 命令会读取 `Makefile` 同目录下的 `configvalue.yml` 文件，并上传到 `configServer`。该文件格式如下：

```bash
$ cat configvalue.yml
rate: 100
scale: 12.2
```

# 在 chainmaker-go 项目中的开发示例

## 添加 `recorderfile.Start(port)` 参数的配置项

- 在 `chainmaker-go/config/config_tpl/chainmaker.tpl` 文件的末尾，添加如下配置内容。这些配置内容默认是被注释掉的。
  ```yml
  #plugin:
  #  # Configs for recorderfile measure
  #  recorder_file:
  #    config_server_port: 9527
  ```
  参考 chainmaker 官方文档 [通过命令行体验链](https://docs.chainmaker.org.cn/v2.3.0/html/quickstart/%E9%80%9A%E8%BF%87%E5%91%BD%E4%BB%A4%E8%A1%8C%E4%BD%93%E9%AA%8C%E9%93%BE.html) 搭建集群的时候，在生成的 release 目录下的各个节点中，应当将`chainmaker.yml`文件末尾的这些注释解除，并将 dns 修改成自己提供的 msyql 的 dns; 如果基于同一份代码生成多个 node，且各个 node 在同一台机器上部署运行，那么需要修改各个 node 的`config_server_port`为不同的值，避免端口冲突。如下所示：
  ```yaml
  plugin:
    # Configs for recorderfile measure
    recorder_file:
      # the port of recorderfile config server, we use it to dynamically update recorderfile config
      config_server_port: 9527
  ```
- 对应于上述的 `plugin` 配置项，在 `git.chainmaker.org.cn/chainmaker/localconf` 仓库的 `types.go` 文件中，新添加如下 `Plugin` 结构:

  ```go
  type CMConfig struct {
      ......
      Plugin          Plugin          `mapstructure:"plugin"`
  }
  type Plugin struct {
      Recorderfile Recorderfile `mapstructure:"recorderfile"`
  }

  type Recorderfile struct {
      ConfigServerPort uint16 `mapstructure:"config_server_port"`
  }
  ```

  `chainmaker-go` 引用了 `localconf` 包，并在 `chainmaker-go/main/cmd/cli_start.go` 代码中的 `initLocalConfig(cmd)` 函数中初始化了 `localconf.ChainMakerConfig` 这个包级别的导出变量。如果在 `chainmaker.yml` 中正确配置了 `plugin` 项目，则该配置项会被解析成 `localconf.ChainMakerConfig.Plugin` 这个字段。

  ```go
  func StartCMD() *cobra.Command {
      startCmd := &cobra.Command{
          ......
          RunE: func(cmd *cobra.Command, _ []string) error {
              initLocalConfig(cmd)
              ......
              return nil
          },
      }
      ......
      return startCmd
  }
  ```

## 初始化 `recorderfile` 包

- 在 `chainmaker-go/main/cmd/cli_start.go:mainStart()` 函数中，添加如下代码，对 `recorderfile` 包进行初始化：

  ```go
  package cmd

  ......

  func mainStart() {
      if localconf.ChainMakerConfig.DebugConfig.IsTraceMemoryUsage {
          traceMemoryUsage()
      }

      if len(localconf.ChainMakerConfig.Plugin.Recorderfile.DNS) > 0 { // 这里初始化 recorderfile 包; 如果注释掉 chainmaker.yml 中的 plugin 配置项，则不会对 recorderfile 包进行初始化。
          port := localconf.ChainMakerConfig.Plugin.Recorderfile.ConfigServerPort
          err := recorderfile.Start(port)
          if err != nil {
              log.Errorf("recorderfile.Start(%s, %d) err: %s", dns, port, err.Error())
          } else {
              log.Infof("recorderfile package connected to mysql")
          }
      }

      // init chainmaker server
      chainMakerServer := blockchain.NewChainMakerServer()
      ......
  }
  ```

  至此，`recorderfile` 就成功 connect 到了 `MySQL`。后续可以使用 `recorderfile.Record()` 函数将数据记录到数据库。如果 `recorderfile` 没有成功 connect 到 `MySQL`，且 `recorderfile.Record()` 的第二个参数 `resultC` 不为 `nil`，则 `recorderfile.Record()` 会向 `resultC` 传入一个 `error`，提示 `"[recorderfile] database not connected"`。

package main

import (
	"os"
	"time"

	"github.com/byteplus-sdk/example-go/common"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/general"
)

var (
	client general.Client

	requestHelper *common.RequestHelper

	concurrentHelper *ConcurrentHelper
)

func initClient() {
	logs.Level = logs.LevelDebug
	client, _ = (&general.ClientBuilder{}).
		// ************* 下述信息需要自行填写 *************
		TenantId(TenantId).   // 必传，租户id
		ProjectId(ProjectId). // 必传，项目id
		AK(AK).               // 必传，密钥AK，请填写自己账户的AK
		SK(SK).               // 必传，密钥AK，请填写自己账户的SK
		// ************* 上述信息需要自行填写 *************
		Region(core.RegionAir).                        // 必传，必须填core.RegionAir，默认使用byteair-api-cn1.snssdk.com为host
		Hosts([]string{"byteair-api-cn1.snssdk.com"}). //可选，如果设置了region则host可不设置
		Schema("https").                               // 可选，仅支持"https"和"http"
		//Headers(map[string]string{"Customer-Header":"value"}). //可选，添加自定义header
		Build()
	requestHelper = &common.RequestHelper{Client: client}
	concurrentHelper = NewConcurrentHelper(client)
}

/**
 * 下面example请求中使用的是demo的参数，可能无法直接请求通过，
 * 需要替换constant.go中相关参数为真实参数
 */
func main() {
	// 初始化client
	initClient()

	// 数据同步示例
	syncExamples()

	// 请求推荐服务获取推荐结果
	recommendExamples()

	// Pause for 5 seconds until the asynchronous import task completes
	time.Sleep(5 * time.Second)
	client.Release()
	os.Exit(0)
}

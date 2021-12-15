package main

import (
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/byteplus-sdk/sdk-go/byteair"

	"github.com/byteplus-sdk/example-go/common"
	. "github.com/byteplus-sdk/sdk-go/byteair/protocol"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultRetryTimes = 2

	DefaultWriteTimeout = 800 * time.Millisecond

	DefaultImportTimeout = 800 * time.Millisecond

	DefaultDoneTimeout = 800 * time.Millisecond

	DefaultPredictTimeout = 800 * time.Millisecond

	DefaultCallbackTimeout = 800 * time.Millisecond
)

var (
	client byteair.Client

	requestHelper *common.RequestHelper

	concurrentHelper *ConcurrentHelper
)

func init() {
	logs.Level = logs.LevelDebug
	client, _ = (&byteair.ClientBuilder{}).
		TenantId(TenantId).                            // 必传，租户id
		ProjectId(ProjectId).                          // 必传，项目id
		AK(AK).                                        // 必传，密钥AK，请填写自己账户的AK
		SK(SK).                                        // 必传，密钥SK，请填写自己账户的SK
		Region(core.RegionAirCn).                      // 必传，必须填core.RegionAir，默认使用byteair-api-cn1.snssdk.com为host
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
	// 实时数据上传
	writeDataExample()
	// 并发实时数据上传
	concurrentWriteDataExample()

	// 标识天级离线数据上传完成
	doneExample()
	// 并发标识天级离线数据上传完成
	concurrentDoneExample()

	// 请求推荐服务获取推荐结果
	recommendExample()
	// 上报回调数据
	callbackExample()

	// Pause for 5 seconds until the asynchronous import task completes
	time.Sleep(5 * time.Second)
	client.Release()
	os.Exit(0)
}

// 数据上传example
func writeDataExample() {
	// 此处为测试数据，实际调用时需注意字段类型和格式
	dataList := mockDataList(2)

	// 同步离线天级数据，需要指定日期
	opts := dailyWriteOptions("2021-11-01")

	// 同步实时数据
	//opts := streamingWriteOptions()

	// topic为枚举值，请参考API文档
	topic := TopicUser
	call := func(dataList interface{}, opts ...option.Option) (proto.Message, error) {
		return client.WriteData(dataList.([]map[string]interface{}), topic, opts...)
	}
	responseItr, err := requestHelper.DoWithRetry(call, dataList, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("[WriteData] occur error, msg:%s", err.Error())
		return
	}
	response := responseItr.(*WriteResponse)
	if common.IsSuccess(response.GetStatus()) {
		logs.Info("[WriteData] success")
		return
	}
	// 出现错误、异常时请记录好日志，方便自行排查问题
	logs.Error("[WriteData] find failure info, msg:%s errItems:%s",
		response.GetStatus(), response.GetErrors())
}

// 增量实时数据并发/异步上传example
func concurrentWriteDataExample() {
	dataList := mockDataList(2)
	topic := TopicUser
	opts := dailyWriteOptions("2021-11-01")
	concurrentHelper.submitWriteRequest(dataList, topic, opts...)
}

// 实时数据同步write参数构造，需要传入日期，e.g. 2021-10-01
func streamingWriteOptions() []option.Option {
	//customHeaders := map[string]string{}
	return []option.Option{
		// 必选. Write接口只能用于实时数据传输，此处只能填"incremental_sync_streaming"
		option.WithStage(StageIncrementalSyncStreaming),
		// 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
		option.WithRequestId(uuid.NewString()),
		// 可选，请求超时时间，可根据实际情况修改
		option.WithTimeout(DefaultWriteTimeout),
		// 可选. 服务端期望在一定时间内返回，避免客户端超时前响应无法返回。
		// 此服务器超时应小于Write请求设置的总超时。
		option.WithServerTimeout(DefaultWriteTimeout - 100*time.Millisecond),
		// 可选. 添加自定义header.
		//option.WithHeaders(customHeaders),
	}
}

// 离线数据同步write参数构造，需要传入日期，e.g. 2021-10-01
func dailyWriteOptions(dateStr string) []option.Option {
	//customHeaders := map[string]string{}
	date, _ := time.Parse("2006-01-02", dateStr)
	return []option.Option{
		// 必选， Import接口数据传输阶段，包括：
		// 测试数据/预同步阶段（"pre_sync"）、历史数据同步（"history_sync"）和增量天级数据上传（"incremental_sync_daily"）
		option.WithStage(StagePreSync),
		// 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
		option.WithRequestId(uuid.NewString()),
		// 必传，数据产生日期，实际传输时需修改为实际日期
		option.WithDataDate(date),
		// 可选，请求超时时间
		option.WithTimeout(DefaultImportTimeout),
		// 可选. 添加自定义header.
		//option.WithHeaders(customHeaders),
	}
}

// 离线天级数据上传完成后Done接口example
func doneExample() {
	date, _ := time.Parse("20060102", "20210908")
	// 已经上传完成的数据日期，可在一次请求中传多个
	dateList := []time.Time{date}
	// 与离线天级数据传输的topic保持一致
	topic := TopicUser
	opts := doneOptions()
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return client.Done(dateList, topic, opts...)
	}
	responseItr, err := requestHelper.DoWithRetry(call, dateList, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("[Done] occur error, msg:%s", err.Error())
		return
	}
	response := responseItr.(*DoneResponse)
	if common.IsSuccess(response.GetStatus()) {
		logs.Info("[Done] success")
		return
	}
	logs.Error("[Done] find failure info, rsp:%s", response)
}

// 离线天级数据上传完成后异步Done接口example，done接口一般无需异步
func concurrentDoneExample() {
	date, _ := time.Parse("20060102", "20200610")
	dateList := []time.Time{date}
	topic := TopicUser
	opts := doneOptions()
	concurrentHelper.submitDoneRequest(dateList, topic, opts...)
}

// Done请求参数说明，请根据说明修改
func doneOptions() []option.Option {
	//customHeaders := map[string]string{}
	return []option.Option{
		// 必选，与Import接口数据传输阶段保持一致，包括：
		// 测试数据/预同步阶段（"pre_sync"）、历史数据同步（"history_sync"）和增量天级数据上传（"incremental_sync_daily"）
		option.WithStage(StagePreSync),
		// 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
		option.WithRequestId(uuid.NewString()),
		// 可选，请求超时时间
		option.WithTimeout(DefaultDoneTimeout),
		// 可选. 添加自定义header.
		//option.WithHeaders(customHeaders),
	}
}

func buildPredictRequest() *PredictRequest {
	user := &PredictUser{
		Uid: "uid",
	}
	context := &PredictContext{
		Spm: "xx$$xxx$$xx",
	}
	candidateItem := &PredictCandidateItem{
		Id: "item_id",
	}
	relatedItem := &PredictRelatedItem{
		Id: "item_id",
	}
	extra := &PredictExtra{
		Extra: map[string]string{"extra_key": "value"},
	}
	return &PredictRequest{
		User:           user,
		Context:        context,
		Size:           20,
		CandidateItems: []*PredictCandidateItem{candidateItem},
		RelatedItem:    relatedItem,
		Extra:          extra,
	}
}

// 推荐服务请求example
func recommendExample() {
	predictRequest := buildPredictRequest()
	predictOpts := defaultOptions(DefaultPredictTimeout)
	scene := "home"
	// who according to tenant's situation
	// The `scene` is provided by ByteDance,
	predictOpts = append(predictOpts, option.WithScene(scene))
	predictResponse, err := client.Predict(predictRequest, predictOpts...)
	if err != nil {
		logs.Error("predict occur error, msg:%s", err.Error())
		return
	}
	if !common.IsSuccessCode(predictResponse.GetCode()) {
		logs.Error("predict find failure info, msg:%s", predictResponse)
		return
	}
	logs.Info("predict success")
}

func callbackExample() {
	// set request and response of recommend api
	var predictRequest *PredictRequest
	var predictResponse *PredictResponse

	// set scene
	scene := "home"

	// The items, which is eventually shown to user,
	// should send back to Bytedance for deduplication
	callbackItems := conv2CallbackItems(predictResponse.GetValue().GetItems())
	callbackRequest := &CallbackRequest{
		PredictRequestId: predictResponse.GetRequestId(),
		Uid:              predictRequest.GetUser().GetUid(),
		Scene:            scene,
		Items:            callbackItems,
	}
	ackOpts := defaultOptions(DefaultCallbackTimeout)
	callbackResponse, err := client.Callback(callbackRequest, ackOpts...)
	if err != nil {
		logs.Error("callback occur error, msg:%s", err.Error())
		return
	}
	if !common.IsSuccessCode(callbackResponse.GetCode()) {
		logs.Error("callback find failure info, msg:%s", callbackResponse)
		return
	}
	logs.Info("callback success")
}

func defaultOptions(timeout time.Duration) []option.Option {
	// All options are optional
	//var customerHeaders map[string]string
	opts := []option.Option{
		option.WithRequestId(uuid.NewString()),
		option.WithTimeout(timeout),
		//option.WithHeaders(customerHeaders),
	}
	return opts
}

func conv2CallbackItems(resultItems []*PredictResultItem) []*CallbackItem {
	if len(resultItems) == 0 {
		return nil
	}
	callbackItems := make([]*CallbackItem, len(resultItems))
	for i, l := 0, len(callbackItems); i < l; i++ {
		resultItem := resultItems[i]
		extraMap := map[string]string{"reason": "kept"}
		extraJsonBytes, _ := json.Marshal(extraMap)
		callbackItem := &CallbackItem{
			Id:    resultItem.GetId(),
			Pos:   strconv.Itoa(i + 1),
			Extra: string(extraJsonBytes),
		}
		callbackItems[i] = callbackItem
	}
	return callbackItems
}

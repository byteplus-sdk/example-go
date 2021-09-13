package main

import (
	"encoding/json"
	"os"
	"strconv"
	"time"

	"github.com/byteplus-sdk/example-go/common"
	. "github.com/byteplus-sdk/sdk-go/common/protocol"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"github.com/byteplus-sdk/sdk-go/general"
	. "github.com/byteplus-sdk/sdk-go/general/protocol"
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
	client general.Client

	requestHelper *common.RequestHelper

	concurrentHelper *ConcurrentHelper
)

const (
	/*
	 * 租户相关信息
	 */
	// Token 字节侧提供，用于签名
	Token = "xxxxxxxxxxxxxxxxxxxxx"

	// TenantId 火山引擎申请的账号id/租户id(tenant_id)，如"2100021"
	TenantId = "xxxxxxxxxxxx"

	// ProjectId 个性化推荐服务新建的项目id(project_id)，如"1231314"
	ProjectId = "xxxxxxxxxxx"

	/*
	 * stage枚举值，与推荐平台四种同步阶段相对应
	 */
	// StageIncrementalSyncStreaming 增量实时数据同步阶段
	StageIncrementalSyncStreaming = "incremental_sync_streaming"

	// StageIncrementalSyncDaily 增量天级数据同步阶段
	StageIncrementalSyncDaily = "incremental_sync_daily"

	// StagePreSync 测试数据/预同步阶段
	StagePreSync = "pre_sync"

	// StageHistorySync 历史数据同步阶段
	StageHistorySync = "history_sync"

	/*
	 * 标准数据topic枚举值，包括：item(物品，如商品、媒资数据、社区内容等)、user(用户)、behavior(行为)
	 */
	// TopicItem 物品
	TopicItem = "item"

	// TopicUser 用户
	TopicUser = "user"

	// TopicBehavior 行为
	TopicBehavior = "behavior"
)

func init() {
	logs.Level = logs.LevelDebug
	client, _ = (&general.ClientBuilder{}).
		Tenant(ProjectId).        // 必传，项目id
		TenantId(TenantId).       // 必传，租户id
		Token(Token).             // 必传，签名token
		Region(core.RegionAirSg). // 必传，必须填core.RegionAir，默认使用byteair-api-cn1.snssdk.com为host
		//Hosts([]string{"byteair-api-cn1.snssdk.com"}). //可选，如果设置了region则host可不设置
		//Schema("https").                               // 可选，仅支持"https"和"http"
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

	// 天级离线数据上传
	importDataExample()
	// 并发天级离线数据上传
	concurrentImportDataExample()

	// 标识天级离线数据上传完成
	doneExample()
	// 并发标识天级离线数据上传完成
	concurrentDoneExample()

	// 与Import接口一起使用，用于天级数据上传状态（是否处理完成，成功/失败条数）监听
	getOperationExample()

	// 请求推荐服务获取推荐结果
	recommendExample()

	// Pause for 5 seconds until the asynchronous import task completes
	time.Sleep(5 * time.Second)
	client.Release()
	os.Exit(0)
}

// 增量实时数据上传example
func writeDataExample() {
	// 此处为测试数据，实际调用时需注意字段类型和格式
	dataList := mockDataList(2)
	opts := writeOptions()
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
	opts := writeOptions()
	concurrentHelper.submitWriteRequest(dataList, topic, opts...)
}

// Write请求参数说明，请根据说明修改
func writeOptions() []option.Option {
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

// 离线天级数据上传example
func importDataExample() {
	// 一个“Import”请求中包含的数据条数最多为10k，如果数据太多，服务器将拒绝请求。
	dataList := mockDataList(2)
	opts := importOptions()
	topic := TopicUser
	call := func(dataList interface{}, opts ...option.Option) (proto.Message, error) {
		return client.ImportData(dataList.([]map[string]interface{}), topic, opts...)
	}
	response := &ImportResponse{}
	err := requestHelper.DoImport(call, dataList, response, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("[ImportData] occur error, msg:%s", err.Error())
		return
	}
	if common.IsSuccess(response.GetStatus()) {
		logs.Info("[ImportData] success")
		return
	}
	logs.Error("[ImportData] find failure info, msg:%s errSamples:%s",
		response.GetStatus(), response.GetErrorSamples())
}

// 离线天级数据并发/异步上传example
func concurrentImportDataExample() {
	dataList := mockDataList(2)
	topic := TopicUser
	opts := importOptions()
	concurrentHelper.submitImportRequest(dataList, topic, opts...)
}

// Import请求参数说明，请根据说明修改
func importOptions() []option.Option {
	//customHeaders := map[string]string{}
	date, _ := time.Parse("2006-01-02", "2021-09-08")
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

// getOperation接口使用example，一般与Import接口一起使用，用于天级数据上传状态监听
func getOperationExample() {
	common.GetOperationExample(client, "0c5a1145-2c12-4b83-8998-2ae8153ca089")
}

// 推荐服务请求example
func recommendExample() {
	predictRequest := buildPredictRequest()
	predictOpts := defaultOptions(DefaultPredictTimeout)
	// The `scene` is provided by ByteDance,
	// who according to tenant's situation
	scene := "home"
	predictResponse, err := client.Predict(predictRequest, scene, predictOpts...)
	if err != nil {
		logs.Error("predict occur error, msg:%s", err.Error())
		return
	}
	if !common.IsSuccessCode(predictResponse.GetCode()) {
		logs.Error("predict find failure info, msg:%s", predictResponse)
		return
	}
	logs.Info("predict success")
	// The items, which is eventually shown to user,
	// should send back to Bytedance for deduplication
	callbackItems := doSomethingWithPredictResult(predictResponse.GetValue())
	callbackRequest := &CallbackRequest{
		PredictRequestId: predictResponse.GetRequestId(),
		Uid:              predictRequest.GetUser().GetUid(),
		Scene:            scene,
		Items:            callbackItems,
	}
	ackOpts := defaultOptions(DefaultCallbackTimeout)
	concurrentHelper.submitCallbackRequest(callbackRequest, ackOpts...)
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

func doSomethingWithPredictResult(predictResult *PredictResult) []*CallbackItem {
	// You can handle recommend results here,
	// such as filter, insert other items, sort again, etc.
	// The list of goods finally displayed to user and the filtered goods
	// should be sent back to bytedance for deduplication
	return conv2CallbackItems(predictResult.GetItems())
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

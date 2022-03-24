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

	DefaultDoneTimeout = 800 * time.Millisecond

	DefaultPredictTimeout = 800 * time.Millisecond

	DefaultCallbackTimeout = 800 * time.Millisecond
)

var (
	client general.Client

	requestHelper *common.RequestHelper
)

const (
	// Token
	// A unique token assigned by bytedance, which is used to
	// generate an authenticated signature when building a request.
	// It is sometimes called "secret".
	Token = "xxxxxxxxxxxxxxxxxxxxx"

	// TenantId
	// A unique ID assigned by Bytedance, which is used to
	// generate an authenticated signature when building a request
	// It is sometimes called "appkey".
	TenantId = "xxxxxxxxxxxx"

	// Tenant
	// A unique identity assigned by Bytedance, which is need to fill in URL.
	// It is sometimes called "company".
	Tenant = "general_demo"
)

func init() {
	logs.Level = logs.LevelDebug
	client, _ = (&general.ClientBuilder{}).
		Tenant(Tenant).        // Required
		TenantId(TenantId).    // Required
		Token(Token).          // Required
		Region(core.RegionCn). // Required
		Build()
	requestHelper = &common.RequestHelper{Client: client}
}

/**
 * Those examples request server with account named 'retail_demo',
 * The data in the "demo" account is only used for testing
 * and communication between customers.
 * Please don't send your private data by "demo" account.
 * If you need to send your private data,
 * you can change account to yours here: {@link Constant}
 */
func main() {
	// Write real-time user data
	writeDataExample()

	// Mark some day's data has been entirely imported
	doneExample()

	// Get recommendation results
	recommendExample()

	// Do search request
	searchExample()

	// Pause for 5 seconds until the asynchronous import task completes
	time.Sleep(5 * time.Second)
	client.Release()
	os.Exit(0)
}

func writeDataExample() {
	// The count of items included in one "Write" request
	// is better to less than 10000 when upload data.
	dataList := mockDataList(2)
	opts := writeOptions()
	// The `topic` is some enums provided by bytedance,
	// who according to tenant's situation
	topic := "user"
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
	logs.Error("[WriteData] find failure info, msg:%s errItems:%s",
		response.GetStatus(), response.GetErrors())
}

func writeOptions() []option.Option {
	date, _ := time.Parse("2006-01-02", "2021-08-27")
	return []option.Option{
		option.WithRequestId(uuid.NewString()),
		option.WithTimeout(DefaultWriteTimeout),
		// The date of uploaded data
		// Incremental data uploading: required.
		// Historical data and real-time data uploading: not required.
		option.WithDataDate(date),
		// The server is expected to return within a certain period，
		// to prevent can't return before client is timeout
		//option.WithServerTimeout(DefaultWriteTimeout - 100*time.Millisecond),
	}
}

func doneExample() {
	date, _ := time.Parse("20060102", "20200610")
	dateList := []time.Time{date}
	// The `topic` is some enums provided by bytedance,
	// who according to tenant's situation
	topic := "user"
	opts := defaultOptions(DefaultDoneTimeout)
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
	//callbackExample(scene, predictRequest, predictResponse)
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

func callbackExample(scene string, predictRequest *PredictRequest, predictResponse *PredictResponse)  {
	callbackItems := doSomethingWithPredictResult(predictResponse.GetValue())
	callbackRequest := &CallbackRequest{
		PredictRequestId: predictResponse.GetRequestId(),
		Uid:              predictRequest.GetUser().GetUid(),
		Scene:            scene,
		Items:            callbackItems,
	}
	opts := defaultOptions(DefaultCallbackTimeout)
	callbackResponse, err := client.Callback(callbackRequest, opts...)
	if err != nil {
		logs.Error("[Callback] occur error, msg:%s", err.Error())
		return
	}
	if common.IsSuccessCode(callbackResponse.GetCode()) {
		logs.Info("[Callback] success")
		return
	}
	logs.Error("[Callback] fail, rsp:\n%s", callbackResponse)
}

func doSomethingWithPredictResult(predictResult *PredictResult) []*CallbackItem {
	// You can handle recommend results here,
	// such as filter, insert other items, sort again, etc.
	// The list of goods finally displayed to user and the filtered goods
	// should be sent back to bytedance for deduplication
	return conv2CallbackItems(predictResult.GetItems())
}

func conv2CallbackItems(resultItems []*PredictItem) []*CallbackItem {
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

func searchExample() {
	predictRequest := buildSearchRequest()
	opts := defaultOptions(DefaultPredictTimeout)
	// The `scene` is provided by ByteDance,
	// that usually is "search" in search request
	scene := "search"
	predictResponse, err := client.Predict(predictRequest, scene, opts...)
	if err != nil {
		logs.Error("search occur error, msg:%s", err.Error())
		return
	}
	if !common.IsSuccessCode(predictResponse.GetCode()) {
		logs.Error("search find failure info, msg:%s", predictResponse)
		return
	}
	logs.Info("search success")
}

func buildSearchRequest() *PredictRequest {
	condition := &SearchInfo{
		SearchType: 0,
		Query:      "adidas",
	}
	extra := &PredictExtra{
		Extra: map[string]string{"extra_key": "value"},
	}
	return &PredictRequest{
		Size:            20,
		SearchInfo: condition,
		Extra:           extra,
	}
}

func defaultOptions(timeout time.Duration) []option.Option {
	opts := []option.Option{
		option.WithRequestId(uuid.NewString()),
		option.WithTimeout(timeout),
	}
	return opts
}

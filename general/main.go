package main

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
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

func init() {
	logs.Level = logs.LevelDebug
	client, _ = (&general.ClientBuilder{}).
		Tenant(Tenant).        // Required
		TenantId(TenantId).    // Required
		Token(Token).          // Required
		Region(core.RegionCn). // Required
		//Schema("http"). // Optional
		//HostHeader("rec-b.volcengineapi.com"). // Optional
		//Hosts([]string{"221.194.131.24", "221.194.131.25"}).
		Build()
	requestHelper = &common.RequestHelper{Client: client}
	concurrentHelper = NewConcurrentHelper(client)
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
	// Write real-time data concurrently
	concurrentWriteDataExample()

	// Import daily offline data
	importDataExample()
	// Import daily offline data concurrently
	concurrentImportDataExample()

	// Mark some day's data has been entirely imported
	doneExample()
	// Do 'done' request concurrently
	concurrentDoneExample()

	// Obtain Operation information according to operationName,
	// if the corresponding task is executing, the real-time
	// result of task execution will be returned
	getOperationExample()

	// Lists operations that match the specified filter in the request.
	// It can be used to retrieve the task when losing 'operation.name',
	// or to statistic the execution of the task within the specified range,
	// for example, the total count of successfully imported data.
	// The result of "listOperations" is not real-time.
	// The real-time info should be obtained through "getOperation"
	listOperationsExample()

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
	// The `topic` is some enums provided by bytedance,
	// who according to tenant's situation
	topic := "user_event"
	call := func(dataList interface{}, opts ...option.Option) (proto.Message, error) {
		return client.WriteData(dataList.([]map[string]interface{}), topic, opts...)
	}
	// The count of items included in one "Write" request
	// is better to less than 100 when upload real-time data.
	dataList := mockDataList(2)
	opts := writeOptions()
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

func concurrentWriteDataExample() {
	// The count of items included in one "Write" request
	// is better to less than 100 when upload real-time data.
	dataList := mockDataList(2)
	// The `topic` is some enums provided by bytedance,
	// who according to tenant's situation
	topic := "user_event"
	opts := writeOptions()
	concurrentHelper.submitWriteRequest(dataList, topic, opts...)
}

func writeOptions() []option.Option {
	// All options are optional
	//customerHeaders := map[string]string{}
	return []option.Option{
		option.WithRequestId(uuid.NewString()),
		option.WithTimeout(DefaultWriteTimeout),
		//option.WithHeaders(customerHeaders),
		// The server is expected to return within a certain period，
		// to prevent can't return before client is timeout
		option.WithServerTimeout(DefaultWriteTimeout - 100*time.Millisecond),
	}
}

func importDataExample() {
	// The `topic` is some enums provided by bytedance,
	// who according to tenant's situation
	topic := "user_event"
	call := func(dataList interface{}, opts ...option.Option) (proto.Message, error) {
		return client.ImportData(dataList.([]map[string]interface{}), topic, opts...)
	}
	// The count of items included in one "Import" request is max to 10k.
	// The server will reject request if items are too many.
	dataList := mockDataList(2)
	response := &ImportResponse{}
	opts := importOptions()
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

func concurrentImportDataExample() {
	// The count of items included in one "Import" request is max to 10k.
	// The server will reject request if items are too many.
	dataList := mockDataList(2)
	// The `topic` is some enums provided by bytedance,
	// who according to tenant's situation
	topic := "user_event"
	opts := importOptions()
	concurrentHelper.submitImportRequest(dataList, topic, opts...)
}

func importOptions() []option.Option {
	// All options are optional
	//customerHeaders := map[string]string{}
	return []option.Option{
		option.WithRequestId(uuid.NewString()),
		option.WithTimeout(DefaultImportTimeout),
		//option.WithHeaders(customerHeaders),
		// Required for import request
		// The date in produced of data in this 'import' request
		option.WithDataDate(time.Now()),
		// If data in a whole day has been imported completely,
		// the import request need be with this option
		//option.WithDateEnd(true),
	}
}

func doneExample() {
	date, _ := time.Parse("20060201", "20200610")
	dateList := []time.Time{date}
	// The `topic` is some enums provided by bytedance,
	// who according to tenant's situation
	topic := "user_event"
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

func concurrentDoneExample() {
	date, _ := time.Parse("20060201", "20200610")
	dateList := []time.Time{date}
	// The `topic` is some enums provided by bytedance,
	// who according to tenant's situation
	topic := "user_event"
	opts := defaultOptions(DefaultDoneTimeout)
	concurrentHelper.submitDoneRequest(dateList, topic, opts...)
}

func getOperationExample() {
	common.GetOperationExample(client, "0c5a1145-2c12-4b83-8998-2ae8153ca089")
}

func listOperationsExample() {
	filter := "date>=2021-06-15 and done=true"
	operations := common.ListOperationsExample(client, filter)
	if len(operations) == 0 {
		return
	}
	parseTaskResponse(operations)
}

func parseTaskResponse(operations []*Operation) {
	if len(operations) == 0 {
		return
	}
	for _, operation := range operations {
		if !operation.Done {
			continue
		}
		responseAny := operation.GetResponse()
		typeUrl := responseAny.GetTypeUrl()
		var err error
		// To ensure compatibility, do not parse response by 'Any.unpack()'
		if strings.Contains(typeUrl, "ImportResponse") {
			response := &ImportResponse{}
			err = proto.Unmarshal(responseAny.GetValue(), response)
			if err == nil {
				logs.Info("[ListOperations] import rsp:\n%s", response)
			}
		} else {
			logs.Error("[ListOperations] unexpected task response type:%s", typeUrl)
			return
		}
		if err != nil {
			logs.Error("[ListOperations] parse task response fail, msg:%s", err.Error())
		}
	}
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
	condition := &SearchCondition{
		SearchType: 0,
		Query:      "adidas",
	}
	extra := &PredictExtra{
		Extra: map[string]string{"extra_key": "value"},
	}
	return &PredictRequest{
		Size:            20,
		SearchCondition: condition,
		Extra:           extra,
	}
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

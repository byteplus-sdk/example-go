package main

import (
	"os"
	"strings"
	"time"

	"github.com/byteplus-sdk/example-go/common"
	. "github.com/byteplus-sdk/sdk-go/common/protocol"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"github.com/byteplus-sdk/sdk-go/retail"
	. "github.com/byteplus-sdk/sdk-go/retail/protocol"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultRetryTimes = 2

	DefaultWriteTimeout = 800 * time.Millisecond

	DefaultImportTimeout = 800 * time.Millisecond

	DefaultPredictTimeout = 800 * time.Millisecond

	DefaultAckImpressionsTimeout = 800 * time.Millisecond
)

var (
	client retail.Client

	requestHelper *common.RequestHelper

	concurrentHelper *ConcurrentHelper
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
	Tenant = "retail_demo"
)

func init() {
	logs.Level = logs.LevelDebug
	client, _ = (&retail.ClientBuilder{}).
		Tenant(Tenant).        // Required
		TenantId(TenantId).    // Required
		Token(Token).          // Required
		Region(core.RegionSg). // Required
		//Schema("https"). // Optional
		//Headers(map[string]string{"Customer-Header": "Value"}). // Optional
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
	writeUsersExample()
	// Write real-time user data concurrently
	concurrentWriteUsersExample()
	// Import daily offline user data
	importUsersExample()
	// Import daily offline user data concurrently
	concurrentImportUsersExample()

	// Write real-time product data˚
	writeProductsExample()
	// Write real-time product data concurrently
	concurrentWriteProductsExample()
	// Import daily offline product data
	importProductsExample()
	// Concurrent import daily offline product data
	concurrentImportProductsExample()

	// Write real-time user event data
	writeUserEventsExample()
	// Write real-time user event data concurrently
	concurrentWriteUserEventsExample()
	// Import daily offline user event data
	importUserEventsExample()
	// Concurrent import daily offline user event data
	concurrentImportUserEventsExample()

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

	// Pause for 5 seconds until the asynchronous import task completes
	time.Sleep(5 * time.Second)
	client.Release()
	os.Exit(0)
}

func writeUsersExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteUsersRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return client.WriteUsers(request.(*WriteUsersRequest), opts...)
	}
	responseItr, err := requestHelper.DoWithRetry(call, request, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("write user occur err, msg:%s", err.Error())
		return
	}
	response := responseItr.(*WriteUsersResponse)
	if common.IsUploadSuccess(response.GetStatus()) {
		logs.Info("write user success")
		return
	}
	logs.Error("write user find failure info, msg:%s errItems:%+v",
		response.GetStatus(), response.GetErrors())
}

func concurrentWriteUsersExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteUsersRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	_ = concurrentHelper.SubmitRequest(request, opts...)
}

func buildWriteUsersRequest(count int) *WriteUsersRequest {
	users := mockUsers(count)
	return &WriteUsersRequest{
		Users: users,
		Extra: map[string]string{"extra_info": "extra"},
	}
}

func importUsersExample() {
	// The "ImportXXX" api can transfer max to 10k items at one request
	request := buildImportUsersRequest(10)
	opts := defaultOptions(DefaultImportTimeout)
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return client.ImportUsers(request.(*ImportUsersRequest), opts...)
	}
	response := &ImportUsersResponse{}
	err := requestHelper.DoImport(call, request, response, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("import user occur err, msg:%s", err.Error())
		return
	}
	if common.IsSuccess(response.GetStatus()) {
		logs.Info("import user success")
		return
	}
	logs.Error("import user find failure info, msg:%s errSamples:%s",
		response.GetStatus(), response.GetErrorSamples())
}

func concurrentImportUsersExample() {
	// The "ImportXXX" api can transfer max to 10k items at one request
	request := buildImportUsersRequest(10)
	opts := defaultOptions(DefaultImportTimeout)
	_ = concurrentHelper.SubmitRequest(request, opts...)
}

func buildImportUsersRequest(count int) *ImportUsersRequest {
	inlineSource := &UsersInputConfig_UsersInlineSource{
		UsersInlineSource: &UsersInlineSource{
			Users: mockUsers(count),
		}}
	inputConfig := &UsersInputConfig{
		Source: inlineSource,
	}

	dateConfig := &DateConfig{
		Date:  time.Now().Format(time.RFC3339),
		IsEnd: false,
	}

	return &ImportUsersRequest{
		InputConfig: inputConfig,
		DateConfig:  dateConfig,
	}
}

func writeProductsExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteProductsRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return client.WriteProducts(request.(*WriteProductsRequest), opts...)
	}
	responseItr, err := requestHelper.DoWithRetry(call, request, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("write product occur err, msg:%s", err.Error())
		return
	}
	response := responseItr.(*WriteProductsResponse)
	if common.IsUploadSuccess(response.GetStatus()) {
		logs.Info("write product success")
		return
	}
	logs.Error("write product find failure info, msg:%s errItems:%+v",
		response.GetStatus(), response.GetErrors())
}

func concurrentWriteProductsExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteProductsRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	_ = concurrentHelper.SubmitRequest(request, opts...)
}

func buildWriteProductsRequest(count int) *WriteProductsRequest {
	products := mockProducts(count)
	return &WriteProductsRequest{
		Products: products,
		Extra:    map[string]string{"extra_info": "extra"},
	}
}

func importProductsExample() {
	// The "ImportXXX" api can transfer max to 10k items at one request
	request := buildImportProductsRequest(10)
	opts := defaultOptions(DefaultImportTimeout)
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return client.ImportProducts(request.(*ImportProductsRequest), opts...)
	}
	response := &ImportProductsResponse{}
	err := requestHelper.DoImport(call, request, response, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("import product occur err, msg:%s", err.Error())
		return
	}
	if common.IsSuccess(response.GetStatus()) {
		logs.Info("import product success")
		return
	}
	logs.Error("import product find failure info, msg:%s errSamples:%s",
		response.GetStatus(), response.GetErrorSamples())
}

func concurrentImportProductsExample() {
	// The "ImportXXX" api can transfer max to 10k items at one request
	request := buildImportProductsRequest(10)
	opts := defaultOptions(DefaultImportTimeout)
	_ = concurrentHelper.SubmitRequest(request, opts...)
}

func buildImportProductsRequest(count int) *ImportProductsRequest {
	inlineSource := &ProductsInputConfig_ProductsInlineSource{
		ProductsInlineSource: &ProductsInlineSource{
			Products: mockProducts(count),
		}}
	inputConfig := &ProductsInputConfig{
		Source: inlineSource,
	}

	dateConfig := &DateConfig{
		Date:  time.Now().Format(time.RFC3339),
		IsEnd: false,
	}

	return &ImportProductsRequest{
		InputConfig: inputConfig,
		DateConfig:  dateConfig,
	}
}

func writeUserEventsExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteUserEventsRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return client.WriteUserEvents(request.(*WriteUserEventsRequest), opts...)
	}
	responseItr, err := requestHelper.DoWithRetry(call, request, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("write user event occur err, msg:%s", err.Error())
		return
	}
	response := responseItr.(*WriteUserEventsResponse)
	if common.IsUploadSuccess(response.GetStatus()) {
		logs.Info("write user event success")
		return
	}
	logs.Error("write user event find failure info, msg:%s errItems:%+v",
		response.GetStatus(), response.GetErrors())
}

func concurrentWriteUserEventsExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteUserEventsRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	_ = concurrentHelper.SubmitRequest(request, opts...)
}

func buildWriteUserEventsRequest(count int) *WriteUserEventsRequest {
	userEvents := mockUserEvents(count)
	return &WriteUserEventsRequest{
		UserEvents: userEvents,
		Extra:      map[string]string{"extra_info": "extra"},
	}
}

func importUserEventsExample() {
	// The "ImportXXX" api can transfer max to 10k items at one request
	request := buildImportUserEventsRequest(10)
	opts := defaultOptions(DefaultImportTimeout)
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return client.ImportUserEvents(request.(*ImportUserEventsRequest), opts...)
	}
	response := &ImportUserEventsResponse{}
	err := requestHelper.DoImport(call, request, response, opts, DefaultRetryTimes)
	if err != nil {
		logs.Error("import user event occur err, msg:%s", err.Error())
		return
	}
	if common.IsSuccess(response.GetStatus()) {
		logs.Info("import user event success")
		return
	}
	logs.Error("import user event find failure info, msg:%s errSamples:%s",
		response.GetStatus(), response.GetErrorSamples())
}

func concurrentImportUserEventsExample() {
	// The "ImportXXX" api can transfer max to 10k items at one request
	request := buildImportUserEventsRequest(10)
	opts := defaultOptions(DefaultImportTimeout)
	_ = concurrentHelper.SubmitRequest(request, opts...)
}

func buildImportUserEventsRequest(count int) *ImportUserEventsRequest {
	inlineSource := &UserEventsInputConfig_UserEventsInlineSource{
		UserEventsInlineSource: &UserEventsInlineSource{
			UserEvents: mockUserEvents(count),
		}}
	inputConfig := &UserEventsInputConfig{
		Source: inlineSource,
	}

	dateConfig := &DateConfig{
		Date:  time.Now().Format(time.RFC3339),
		IsEnd: false,
	}

	return &ImportUserEventsRequest{
		InputConfig: inputConfig,
		DateConfig:  dateConfig,
	}
}

func getOperationExample() {
	common.GetOperationExample(client, "750eca88-5165-4aae-851f-a93b75a27b03")
}

func listOperationsExample() {
	filter := "date>=2021-06-15 and worksOn=ImportUsers and done=true"
	operations := common.ListOperationsExample(client, filter)
	if operations == nil {
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
		if strings.Contains(typeUrl, "ImportUsers") {
			response := &ImportUsersResponse{}
			err = proto.Unmarshal(responseAny.GetValue(), response)
			if err == nil {
				logs.Info("[ListOperations] ImportUsers rsp:\n%s", response)
			}
		} else if strings.Contains(typeUrl, "ImportProducts") {
			response := &ImportProductsResponse{}
			err = proto.Unmarshal(responseAny.GetValue(), response)
			if err == nil {
				logs.Info("[ListOperations] ImportProducts rsp:\n%s", response)
			}
		} else if strings.Contains(typeUrl, "ImportUserEvents") {
			response := &ImportUserEventsResponse{}
			err = proto.Unmarshal(responseAny.GetValue(), response)
			if err == nil {
				logs.Info("[ListOperations] ImportUserEvents rsp:\n%s", response)
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
	// The "home" is scene name, which provided by ByteDance, usually is "home"
	response, err := client.Predict(predictRequest, "home", predictOpts...)
	if err != nil {
		logs.Error("predict occur error, msg:%s", err.Error())
		return
	}
	if !common.IsSuccess(response.GetStatus()) {
		logs.Error("predict find failure info, msg:%s", response.GetStatus())
		return
	}
	logs.Info("predict success")
	// The items, which is eventually shown to user,
	// should send back to Bytedance for deduplication
	alteredProducts := doSomethingWithPredictResult(response.GetValue())
	ackRequest := buildAckRequest(response.GetRequestId(), predictRequest, alteredProducts)
	ackOpts := defaultOptions(DefaultAckImpressionsTimeout)
	_ = concurrentHelper.SubmitRequest(ackRequest, ackOpts...)
}

func buildPredictRequest() *PredictRequest {
	scene := &UserEvent_Scene{
		SceneName: "home",
	}
	rootProduct := mockProduct()
	device := mockDevice()
	context := &PredictRequest_Context{
		RootProduct:         rootProduct,
		Device:              device,
		CandidateProductIds: []string{"pid1", "pid2"},
	}
	return &PredictRequest{
		UserId:  "user_id",
		Size:    20,
		Scene:   scene,
		Context: context,
		Extra:   map[string]string{"page_num": "1"},
	}
}

func doSomethingWithPredictResult(predictResult *PredictResult) []*AckServerImpressionsRequest_AlteredProduct {
	// You can handle recommend results here,
	// such as filter, insert other items, sort again, etc.
	// The list of goods finally displayed to user and the filtered goods
	// should be sent back to bytedance for deduplication
	return conv2AlteredProducts(predictResult.GetResponseProducts())
}

func conv2AlteredProducts(products []*PredictResult_ResponseProduct) []*AckServerImpressionsRequest_AlteredProduct {
	if len(products) == 0 {
		return nil
	}
	alteredProducts := make([]*AckServerImpressionsRequest_AlteredProduct, len(products))
	for i, product := range products {
		alteredProducts[i] = &AckServerImpressionsRequest_AlteredProduct{
			AlteredReason: "kept",
			ProductId:     product.GetProductId(),
			Rank:          int32(i + 1),
		}
	}
	return alteredProducts
}

func buildAckRequest(predictRequestId string, predictRequest *PredictRequest,
	alteredProducts []*AckServerImpressionsRequest_AlteredProduct) *AckServerImpressionsRequest {

	return &AckServerImpressionsRequest{
		PredictRequestId: predictRequestId,
		UserId:           predictRequest.GetUserId(),
		Scene:            predictRequest.GetScene(),
		// If it is the recommendation result from byteplus, traffic_source is byteplus,
		// if it is the customer's own recommendation result, traffic_source is self.
		TrafficSource:   "byteplus",
		AlteredProducts: alteredProducts,
		// Extra:            map[string]string{"ip": "127.0.0.1"},
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

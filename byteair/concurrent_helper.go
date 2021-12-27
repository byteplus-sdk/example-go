package main

import (
	"time"

	"github.com/byteplus-sdk/example-go/common"
	"github.com/byteplus-sdk/sdk-go/byteair"
	. "github.com/byteplus-sdk/sdk-go/byteair/protocol"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"google.golang.org/protobuf/proto"
)

const (
	consumerCount = 5
	retryTimes    = 2
)

func NewConcurrentHelper(client byteair.Client) *ConcurrentHelper {
	taskChan := make(chan runner)
	for i := 0; i < consumerCount; i++ {
		core.AsyncExecute(func() {
			for {
				task := <-taskChan
				task()
			}
		})
	}
	return &ConcurrentHelper{
		client:        client,
		requestHelper: &common.RequestHelper{Client: client},
		taskChan:      taskChan,
	}
}

type runner func()

type ConcurrentHelper struct {
	client        byteair.Client
	requestHelper *common.RequestHelper
	taskChan      chan runner
}

// Submit tasks.
// If the number of imported tasks currently executing exceeds the maximum number
// of concurrent tasks, the commit will be blocked until other task complete.
// Only supported for "import_xxx" and "ack_impressions" request.
// It is recommended to increase the data amount contained in a single request.
// It is not recommended to use too many concurrent imports,
// which may lead to server overload and limit the flow of the request
func (h *ConcurrentHelper) submitWriteRequest(
	dataList []map[string]interface{}, topic string, opts ...option.Option) {

	call := func(dataList interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.WriteData(dataList.([]map[string]interface{}), topic, opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, dataList, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncWriteData] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*WriteResponse).GetStatus()) {
			logs.Info("[AsyncWriteData] success")
			return
		}
		logs.Error("[AsyncWriteData] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

func (h *ConcurrentHelper) submitDoneRequest(
	dataList []time.Time, topic string, opts ...option.Option) {

	call := func(dataList interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.Done(dataList.([]time.Time), topic, opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, dataList, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncDone] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*DoneResponse).GetStatus()) {
			logs.Info("[AsyncDone] success")
			return
		}
		logs.Error("[AsyncDone] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

func (h *ConcurrentHelper) submitCallbackRequest(request *CallbackRequest, opts ...option.Option) {
	call := func(dataList interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.Callback(dataList.(*CallbackRequest), opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, request, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncCallback] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccessCode(response.(*CallbackResponse).GetCode()) {
			logs.Info("[AsyncCallback] success")
			return
		}
		logs.Error("[AsyncCallback] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

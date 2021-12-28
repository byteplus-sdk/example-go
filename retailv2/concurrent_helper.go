package main

import (
	"errors"

	"github.com/byteplus-sdk/example-go/common"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"github.com/byteplus-sdk/sdk-go/retailv2"
	. "github.com/byteplus-sdk/sdk-go/retailv2/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	consumerCount = 5
	retryTimes    = 2
)

func NewConcurrentHelper(client retailv2.Client) *ConcurrentHelper {
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
	client        retailv2.Client
	requestHelper *common.RequestHelper
	taskChan      chan runner
}

func (h *ConcurrentHelper) SubmitRequest(request interface{}, opts ...option.Option) error {
	switch realRequest := request.(type) {
	case *WriteUsersRequest:
		h.submitWriteUsersRequest(realRequest, opts...)
	case *WriteProductsRequest:
		h.submitWriteProductsRequest(realRequest, opts...)
	case *WriteUserEventsRequest:
		h.submitWriteUserEventsRequest(realRequest, opts...)
	case *AckServerImpressionsRequest:
		h.submitAckRequest(realRequest, opts...)
	}
	return errors.New("can't support this request type")
}

func (h *ConcurrentHelper) submitWriteUsersRequest(request *WriteUsersRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.WriteUsers(request.(*WriteUsersRequest), opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, request, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncWriteUsers] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*WriteUsersResponse).GetStatus()) {
			logs.Info("[AsyncWriteUsers] success")
			return
		}
		logs.Error("[AsyncWriteUsers] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

func (h *ConcurrentHelper) submitWriteProductsRequest(request *WriteProductsRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.WriteProducts(request.(*WriteProductsRequest), opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, request, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncWriteProducts] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*WriteProductsResponse).GetStatus()) {
			logs.Info("[AsyncWriteProducts] success")
			return
		}
		logs.Error("[AsyncWriteProducts] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

func (h *ConcurrentHelper) submitWriteUserEventsRequest(request *WriteUserEventsRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.WriteUserEvents(request.(*WriteUserEventsRequest), opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, request, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncWriteUserEvents] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*WriteUserEventsResponse).GetStatus()) {
			logs.Info("[AsyncWriteUserEvents] success")
			return
		}
		logs.Error("[AsyncWriteUserEvents] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

func (h *ConcurrentHelper) submitAckRequest(request *AckServerImpressionsRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.AckServerImpressions(request.(*AckServerImpressionsRequest), opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, request, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncAckImpressions] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*AckServerImpressionsResponse).GetStatus()) {
			logs.Info("[AsyncAckImpressions] success")
			return
		}
		logs.Error("[AsyncAckImpressions] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

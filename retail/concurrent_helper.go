package main

import (
	"errors"
	"github.com/byteplus-sdk/example-go/common"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"github.com/byteplus-sdk/sdk-go/retail"
	. "github.com/byteplus-sdk/sdk-go/retail/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	consumerCount = 5
	retryTimes    = 2
)

func NewConcurrentHelper(client retail.Client) *ConcurrentHelper {
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
	client        retail.Client
	requestHelper *common.RequestHelper
	taskChan      chan runner
}

func (h *ConcurrentHelper) SubmitRequest(request interface{}, opts ...option.Option) error {
	switch realRequest := request.(type) {
	case *WriteUsersRequest:
		h.submitWriteUsersRequest(realRequest, opts...)
	case *ImportUsersRequest:
		h.submitImportUsersRequest(realRequest, opts...)
	case *WriteProductsRequest:
		h.submitWriteProductsRequest(realRequest, opts...)
	case *ImportProductsRequest:
		h.submitImportProductsRequest(realRequest, opts...)
	case *WriteUserEventsRequest:
		h.submitWriteUserEventsRequest(realRequest, opts...)
	case *ImportUserEventsRequest:
		h.submitImportUserEventsRequest(realRequest, opts...)
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

func (h *ConcurrentHelper) submitImportUsersRequest(request *ImportUsersRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.ImportUsers(request.(*ImportUsersRequest), opts...)
	}
	task := func() {
		response := &ImportUsersResponse{}
		err := h.requestHelper.DoImport(call, request, response, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncImportUsers] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.GetStatus()) {
			logs.Info("[AsyncImportUsers] success")
			return
		}
		logs.Error("[AsyncImportUsers] fail, rsp:\n%s", response)
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

func (h *ConcurrentHelper) submitImportProductsRequest(request *ImportProductsRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.ImportProducts(request.(*ImportProductsRequest), opts...)
	}
	task := func() {
		response := &ImportProductsResponse{}
		err := h.requestHelper.DoImport(call, request, response, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncImportProducts] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.GetStatus()) {
			logs.Info("[AsyncImportProducts] success")
			return
		}
		logs.Error("[AsyncImportProducts] fail, rsp:\n%s", response)
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

func (h *ConcurrentHelper) submitImportUserEventsRequest(request *ImportUserEventsRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.ImportUserEvents(request.(*ImportUserEventsRequest), opts...)
	}
	task := func() {
		response := &ImportUserEventsResponse{}
		err := h.requestHelper.DoImport(call, request, response, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncImportUserEvents] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.GetStatus()) {
			logs.Info("[AsyncImportUserEvents] success")
			return
		}
		logs.Error("[AsyncImportUserEvents] fail, rsp:\n%s", response)
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

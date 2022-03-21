package main

import (
	"errors"

	"github.com/byteplus-sdk/example-go/common"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"github.com/byteplus-sdk/sdk-go/media"
	"github.com/byteplus-sdk/sdk-go/media/protocol"
	"google.golang.org/protobuf/proto"
)

const (
	consumerCount = 5
	retryTimes    = 2
)

func NewConcurrentHelper(client media.Client) *ConcurrentHelper {
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
	client        media.Client
	requestHelper *common.RequestHelper
	taskChan      chan runner
}

func (h *ConcurrentHelper) SubmitRequest(request interface{}, opts ...option.Option) error {
	switch realRequest := request.(type) {
	case *protocol.WriteUsersRequest:
		h.submitWriteUsersRequest(realRequest, opts...)
	case *protocol.WriteContentsRequest:
		h.submitWriteContentsRequest(realRequest, opts...)
	case *protocol.WriteUserEventsRequest:
		h.submitWriteUserEventsRequest(realRequest, opts...)
	}
	return errors.New("can't support this request type")
}

func (h *ConcurrentHelper) submitWriteUsersRequest(request *protocol.WriteUsersRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.WriteUsers(request.(*protocol.WriteUsersRequest), opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, request, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncWriteUsers] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*protocol.WriteUsersResponse).GetStatus()) {
			logs.Info("[AsyncWriteUsers] success")
			return
		}
		logs.Error("[AsyncWriteUsers] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

func (h *ConcurrentHelper) submitWriteContentsRequest(request *protocol.WriteContentsRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.WriteContents(request.(*protocol.WriteContentsRequest), opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, request, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncWriteContents] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*protocol.WriteContentsResponse).GetStatus()) {
			logs.Info("[AsyncWriteContents] success")
			return
		}
		logs.Error("[AsyncWriteContents] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

func (h *ConcurrentHelper) submitWriteUserEventsRequest(request *protocol.WriteUserEventsRequest, opts ...option.Option) {
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return h.client.WriteUserEvents(request.(*protocol.WriteUserEventsRequest), opts...)
	}
	task := func() {
		response, err := h.requestHelper.DoWithRetry(call, request, opts, retryTimes)
		if err != nil {
			logs.Error("[AsyncWriteUserEvents] occur error, msg:%s", err.Error())
			return
		}
		if common.IsSuccess(response.(*protocol.WriteUserEventsResponse).GetStatus()) {
			logs.Info("[AsyncWriteUserEvents] success")
			return
		}
		logs.Error("[AsyncWriteUserEvents] fail, rsp:\n%s", response)
	}
	h.taskChan <- task
}

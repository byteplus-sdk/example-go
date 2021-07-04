package common

import (
	"errors"
	"github.com/byteplus-sdk/sdk-go/common"
	. "github.com/byteplus-sdk/sdk-go/common/protocol"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"math"
	"math/rand"
	"reflect"
	"time"
)

const (
	// The maximum time for polling the execution results of the import task
	pollingTimeout = 10 * time.Second

	// The time interval between requests during polling
	pollingInterval = 100 * time.Millisecond

	// The interval base of retry for server overload
	overloadRetryInterval = 200 * time.Millisecond

	getOperationTimeout = 500 * time.Millisecond
)

type Call func(request interface{}, opts ...option.Option) (proto.Message, error)

type RequestHelper struct {
	Client common.Client
}

func (h *RequestHelper) DoImport(call Call, request interface{},
	response proto.Message, opts []option.Option, retryTimes int) error {
	// To ensure that the request is successfully received by the server,
	// it should be retried after network or overload exception occurs.
	opRspItr, err := h.DoWithRetryAlthoughOverload(call, request, opts, retryTimes)
	if err != nil {
		return err
	}
	opRsp := opRspItr.(*OperationResponse)
	if !IsUploadSuccess(opRsp.GetStatus()) {
		logs.Error("[PollingImportResponse] server return error info, rsp:\n%s", opRsp)
		return errors.New("import return failure info")
	}
	return h.pollingResponse(opRsp.GetOperation().GetName(), response)
}

// DoWithRetryAlthoughOverload
// If the task is submitted too fast or the server is overloaded,
// the server may refuse the request. In order to ensure the accuracy
// of data transmission, you should wait some time and request again,
// but it cannot retry endlessly. The maximum count of retries should be set.
//
// @param call	   the task need to execute
// @param request  the request type of task
// @param opts     the options need by the task
// @return error   return by task or server still overload after retry
func (h *RequestHelper) DoWithRetryAlthoughOverload(call Call, request interface{},
	opts []option.Option, retryTimes int) (proto.Message, error) {
	if retryTimes < 0 {
		retryTimes = 0
	}
	tryTimes := retryTimes + 1
	for i := 0; i < tryTimes; i++ {
		response, err := h.DoWithRetry(call, request, opts, retryTimes-i)
		if err != nil {
			return nil, err
		}
		if IsServerOverload(getStatus(response)) {
			// Wait some time before request again,
			// and the wait time will increase by the number of retried
			time.Sleep(randomOverloadWaitTime(i))
			continue
		}
		return response, nil
	}
	return nil, errors.New("server overload")
}

func (h *RequestHelper) DoWithRetry(call Call, request interface{},
	opts []option.Option, retryTimes int) (proto.Message, error) {
	// To ensure the request is successfully received by the server,
	// it should be retried after a network exception occurs.
	// To prevent the retry from causing duplicate uploading same data,
	// the request should be retried by using the same requestId.
	// If a new requestId is used, it will be treated as a new request
	// by the server, which may save duplicate data
	opts = withRequestId(opts)
	if retryTimes < 0 {
		retryTimes = 0
	}
	tryTimes := retryTimes + 1
	for i := 0; i < tryTimes; i++ {
		response, err := call(request, opts...)
		if err != nil {
			if core.IsTimeoutError(err) {
				if i == tryTimes-1 {
					logs.Error("[DoRetryRequest] fail finally after retried {} times", tryTimes)
					return nil, errors.New("still fail after retry")
				}
				continue
			}
			return nil, err
		}
		return response, nil
	}
	return nil, nil
}

func withRequestId(opts []option.Option) []option.Option {
	optsWithRequestId := make([]option.Option, 0, len(opts)+1)
	requestIdOpt := option.WithRequestId(uuid.NewString())
	// This will not override the RequestId set by the user
	optsWithRequestId = append(optsWithRequestId, requestIdOpt)
	optsWithRequestId = append(optsWithRequestId, opts...)
	return optsWithRequestId
}

func getStatus(response interface{}) *Status {
	objValue := reflect.ValueOf(response).Elem()
	statusField := objValue.FieldByName("Status")
	return statusField.Interface().(*Status)
}

func randomOverloadWaitTime(retriedTimes int) time.Duration {
	const increaseSpeed = 3
	if retriedTimes < 0 {
		return overloadRetryInterval
	}
	rate := 1.0 + rand.Float64()*math.Pow(increaseSpeed, float64(retriedTimes))
	return time.Duration(float64(overloadRetryInterval) * rate)
}

func (h *RequestHelper) pollingResponse(name string, response proto.Message) error {
	responseAny, err := h.doPollingResponse(name)
	if err != nil {
		return err
	}
	return proto.Unmarshal(responseAny.Value, response)
}

func (h *RequestHelper) doPollingResponse(name string) (*anypb.Any, error) {
	// Set the polling expiration time to prevent endless polling
	endTime := time.Now().Add(pollingTimeout)
	for ; time.Now().Before(endTime); {
		opRsp, err := h.getPollingOperation(name)
		if err != nil {
			return nil, err
		}
		if opRsp == nil {
			// When polling for import results, you should continue polling
			// until the maximum polling time is exceeded, as long as there is
			// no obvious error that should not continue, such as server telling
			// operation lost, parse response body fail, etc
			continue
		}
		// The server may lose operation information due to unexpected failure.
		// At this time, should interrupt the request and send feedback to bytedance
		// to confirm whether the data in this request has been successfully imported
		if IsLossOperation(opRsp.GetStatus()) {
			logs.Error("[PollingResponse] operation loss, rsp:\n%s", opRsp)
			return nil, errors.New("operation loss")
		}
		op := opRsp.GetOperation()
		// The task corresponding to this operation has been completed,
		// and the execution result  can be obtained through "operation.response"
		if op.Done {
			return op.Response, nil
		}
		// Pause some time to prevent server overload
		time.Sleep(pollingInterval)
	}
	logs.Error("[PollingResponse] timeout after %s", pollingTimeout)
	return nil, errors.New("polling import result timeout")
}

func (h *RequestHelper) getPollingOperation(name string) (*OperationResponse, error) {
	request := &GetOperationRequest{Name: name}
	response, err := h.Client.GetOperation(request, option.WithTimeout(getOperationTimeout))
	if err != nil {
		if core.IsTimeoutError(err) {
			// Should not return the NetException.
			// Return an exception means the request could not continue,
			// while polling for import results should be continue until the
			// maximum polling time is exceeded, as long as there is no obvious
			// error that should not continue, such as server telling operation lost,
			// parse response body fail, etc.
			logs.Warn("[PollingResponse] get operation fail, name:%s msg:%s", name, err.Error())
			return nil, nil
		}
		logs.Error("[PollingResponse] get operation occur error, name:%s msg:%s", name, err.Error())
		return nil, err
	}
	return response, nil
}

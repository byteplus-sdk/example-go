package common

import (
	"github.com/byteplus-sdk/sdk-go/common"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"time"
)
import . "github.com/byteplus-sdk/sdk-go/common/protocol"

const (
	DefaultGetOperationTimeout = 800 * time.Millisecond

	DefaultListOperationsTimeout = 800 * time.Millisecond
)

func GetOperationExample(client common.Client, name string) {
	request := &GetOperationRequest{
		Name: name,
	}
	opts := []option.Option{
		option.WithTimeout(DefaultGetOperationTimeout),
	}
	response, err := client.GetOperation(request, opts...)
	if err != nil {
		logs.Error("get operation occur error, msg:%s", err.Error())
		return
	}
	if IsSuccess(response.GetStatus()) {
		logs.Info("get operation success")
		return
	}
	if IsLossOperation(response.GetStatus()) {
		logs.Error("operation loss, name:%s", request.GetName())
		return
	}
	logs.Error("get operation find failure info, rsp:\n%s", response)
}

func ListOperationsExample(client common.Client, filter string) []*Operation {
	// The "pageToken" is empty when you get the first page
	request := buildListOperationsRequest(filter, "")
	opts := []option.Option{
		option.WithTimeout(DefaultListOperationsTimeout),
	}
	response, err := client.ListOperations(request, opts...)
	if err != nil {
		logs.Error("list operations occur err, msg:%s", err.Error())
		return nil
	}
	if !IsSuccess(response.GetStatus()) {
		logs.Error("list operations find failure info, msg:\n%s", response.GetStatus())
		return nil
	}
	logs.Info("list operations success")
	return response.GetOperations()
	// When you get the next Page, you need to put the "nextPageToken"
	// returned by this Page into the request of next Page
	// nextPageRequest := buildListOperationsRequest(filter, response.GetNextPageToken())
	// request next page
}

func buildListOperationsRequest(filter, pageToken string) *ListOperationsRequest {
	return &ListOperationsRequest{
		Filter:    filter,
		PageSize:  3,
		PageToken: pageToken,
	}
}

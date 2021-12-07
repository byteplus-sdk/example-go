package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/byteplus-sdk/example-go/common"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	. "github.com/byteplus-sdk/sdk-go/general/protocol"
	"github.com/google/uuid"
)

const (
	DefaultPredictTimeout = 800 * time.Millisecond

	DefaultCallbackTimeout = 800 * time.Millisecond
)

func recommendExamples() {
	// 指定场景
	scene := "home"

	// 调用predict接口
	req := buildPredictRequest()
	rsp, err := generalPredict(scene, req)
	if err != nil {
		logs.Error("predict find failure info, msg:%s", err)
	}

	// 处理推荐结果，返回用于callback的items
	callbackItems := doSomethingWithPredictResult(rsp.GetValue())

	// 上报callback
	generalCallback(req.User.GetUid(), scene, rsp.RequestId, callbackItems)
}

// 请求推荐服务获取推荐结果
func generalPredict(scene string, req *PredictRequest) (*PredictResponse, error) {
	predictOpts := defaultOptions(DefaultPredictTimeout)
	// The `scene` is provided by ByteDance,
	// who according to tenant's situation
	predictResponse, err := client.Predict(req, scene, predictOpts...)
	if err != nil {
		return nil, err
	}
	if !common.IsSuccessCode(predictResponse.GetCode()) {
		return nil, fmt.Errorf("%s", predictResponse)
	}
	logs.Info("predict success")
	return predictResponse, nil
}

func generalCallback(uid, scene, reqID string, callbackItems []*CallbackItem) {
	// The items, which is eventually shown to user,
	// should send back to Bytedance for deduplication
	callbackRequest := &CallbackRequest{
		PredictRequestId: reqID,
		Uid:              uid,
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

package main

import (
	"time"

	"github.com/byteplus-sdk/example-go/common"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	. "github.com/byteplus-sdk/sdk-go/general/protocol"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultRetryTimes = 2

	DefaultWriteTimeout = 800 * time.Millisecond

	DefaultDoneTimeout = 800 * time.Millisecond
)

func syncExamples() {
	// 天级离线数据上传，阶段：预同步、历史同步、增量天级
	writeWithDate(StagePreSync, TopicUser, dateOf("2021-10-01"), mockDataList(50))
	// 并发天级离线数据上传
	for i := 0; i < 5; i++ {
		concurrentWriteWithDate(StagePreSync, TopicUser, dateOf("2021-10-01"), mockDataList(10))
	}

	// 标识天级离线数据上传完成
	writeDone(StagePreSync, TopicUser, dateOf("2021-10-01"))
	// 并发标识天级离线数据上传完成
	//concurrentWriteDone(StagePreSync, TopicUser, dateOf("2021-10-01"))

	// 实时数据上传，阶段：增量实时
	writeStreaming(TopicUser, mockDataList(50))
	// 并发实时数据上传
	for i := 0; i < 5; i++ {
		concurrentWriteStreaming(TopicUser, mockDataList(50))
	}
}

// 用于传输离线天级数据，必须指定同步阶段、数据表、日期
func writeWithDate(syncStage, table string, date time.Time, dataList []map[string]interface{}) {
	// 此处为测试数据，实际调用时需注意字段类型和格式
	opts := []option.Option{
		option.WithStage(syncStage),
		// 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
		option.WithRequestId(uuid.NewString()),
		// 可选，请求超时时间，可根据实际情况修改
		option.WithTimeout(DefaultWriteTimeout),
		// 可选. 服务端期望在一定时间内返回，避免客户端超时前响应无法返回。
		// 此服务器超时应小于Write请求设置的总超时。
		option.WithServerTimeout(DefaultWriteTimeout - 100*time.Millisecond),
		option.WithDataDate(date),
		// 可选. 添加自定义header.
		//option.WithHeaders(customHeaders),
	}
	generalWrite(opts, table, dataList)
}

// 用于传输实时数据，无需指定同步阶段，需要指定数据表
func writeStreaming(table string, dataList []map[string]interface{}) {
	// 此处为测试数据，实际调用时需注意字段类型和格式
	opts := []option.Option{
		// 必选. Write接口只能用于实时数据传输，此处只能填"incremental_sync_streaming"
		option.WithStage(StageIncrementalSyncStreaming),
		// 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
		option.WithRequestId(uuid.NewString()),
		// 可选，请求超时时间，可根据实际情况修改
		option.WithTimeout(DefaultWriteTimeout),
		// 可选. 服务端期望在一定时间内返回，避免客户端超时前响应无法返回。
		// 此服务器超时应小于Write请求设置的总超时。
		option.WithServerTimeout(DefaultWriteTimeout - 100*time.Millisecond),
		// 可选. 添加自定义header.
		//option.WithHeaders(customHeaders),
	}
	generalWrite(opts, table, dataList)
}

func generalWrite(opts []option.Option, table string, dataList []map[string]interface{}) {
	// topic为枚举值，请参考API文档
	call := func(dataList interface{}, opts ...option.Option) (proto.Message, error) {
		return client.WriteData(dataList.([]map[string]interface{}), table, opts...)
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
	// 出现错误、异常时请记录好日志，方便自行排查问题
	logs.Error("[WriteData] find failure info, msg:%s errItems:%s",
		response.GetStatus(), response.GetErrors())
}

// 天级离线数据 并发/异步上传
func concurrentWriteWithDate(syncStage, table string, date time.Time, dataList []map[string]interface{}) {
	// 此处为测试数据，实际调用时需注意字段类型和格式
	opts := []option.Option{
		option.WithStage(syncStage),
		// 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
		option.WithRequestId(uuid.NewString()),
		// 可选，请求超时时间，可根据实际情况修改
		option.WithTimeout(DefaultWriteTimeout),
		// 可选. 服务端期望在一定时间内返回，避免客户端超时前响应无法返回。
		// 此服务器超时应小于Write请求设置的总超时。
		option.WithServerTimeout(DefaultWriteTimeout - 100*time.Millisecond),
		option.WithDataDate(date),
		// 可选. 添加自定义header.
		//option.WithHeaders(customHeaders),
	}
	concurrentHelper.submitWriteRequest(dataList, table, opts...)
}

// 实时数据 并发/异步上传
func concurrentWriteStreaming(table string, dataList []map[string]interface{}) {
	// 此处为测试数据，实际调用时需注意字段类型和格式
	opts := []option.Option{
		// 必选. Write接口只能用于实时数据传输，此处只能填"incremental_sync_streaming"
		option.WithStage(StageIncrementalSyncStreaming),
		// 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
		option.WithRequestId(uuid.NewString()),
		// 可选，请求超时时间，可根据实际情况修改
		option.WithTimeout(DefaultWriteTimeout),
		// 可选. 服务端期望在一定时间内返回，避免客户端超时前响应无法返回。
		// 此服务器超时应小于Write请求设置的总超时。
		option.WithServerTimeout(DefaultWriteTimeout - 100*time.Millisecond),
		// 可选. 添加自定义header.
		//option.WithHeaders(customHeaders),
	}
	concurrentHelper.submitWriteRequest(dataList, table, opts...)
}

// utcDateOf returns UTC time.Time of given dateStr, e.g. "2021-10-01")
func utcDateOf(dateStr string) time.Time {
	date, _ := time.Parse("2006-01-02", dateStr)
	return date
}

// dateOf returns time.Local time.Time of given dateStr, e.g. "2021-10-01")
func dateOf(dateStr string) time.Time {
	date, _ := time.ParseInLocation("2006-01-02", dateStr, time.Local)
	return date
}

// 离线天级数据上传完成后Done接口example
func writeDone(syncStage, table string, date time.Time) {
	// 已经上传完成的数据日期，可在一次请求中传多个
	dateList := []time.Time{date}
	// 与离线天级数据传输的topic保持一致
	opts := doneOptions(syncStage)
	call := func(request interface{}, opts ...option.Option) (proto.Message, error) {
		return client.Done(dateList, table, opts...)
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

// 离线天级数据上传完成后异步Done接口example，done接口一般无需异步
func concurrentWriteDone(syncStage, table string, date time.Time) {
	dateList := []time.Time{date}
	concurrentHelper.submitDoneRequest(dateList, table, doneOptions(syncStage)...)
}

// Done请求参数说明，请根据说明修改
func doneOptions(syncStage string) []option.Option {
	//customHeaders := map[string]string{}
	return []option.Option{
		// 必选，与Import接口数据传输阶段保持一致，包括：
		// 测试数据/预同步阶段（"pre_sync"）、历史数据同步（"history_sync"）和增量天级数据上传（"incremental_sync_daily"）
		option.WithStage(syncStage),
		// 必传，要求每次请求的Request-Id不重复，若未传，sdk会默认为每个请求添加
		option.WithRequestId(uuid.NewString()),
		// 可选，请求超时时间
		option.WithTimeout(DefaultDoneTimeout),
		// 可选. 添加自定义header.
		//option.WithHeaders(customHeaders),
	}
}

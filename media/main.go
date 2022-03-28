package main

import (
	"os"
	"time"

	"github.com/byteplus-sdk/example-go/common"
	"github.com/byteplus-sdk/sdk-go/core"
	"github.com/byteplus-sdk/sdk-go/core/logs"
	"github.com/byteplus-sdk/sdk-go/core/option"
	"github.com/byteplus-sdk/sdk-go/media"
	"github.com/byteplus-sdk/sdk-go/media/protocol"
	"github.com/google/uuid"
)

const (
	DefaultWriteTimeout = 800 * time.Millisecond

	DefaultDoneTimeout = 800 * time.Millisecond
)

var (
	client media.Client

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
	Tenant = "media_demo"

	TopicUser      = "user"
	TopicContent   = "content"
	TopicUserEvent = "user_event"
)

func init() {
	logs.Level = logs.LevelDebug
	client, _ = (&media.ClientBuilder{}).
		Tenant(Tenant).        // Required
		TenantId(TenantId).    // Required
		Token(Token).          // Required
		Region(core.RegionSg). // Required
		//Schema("https"). // Optional
		//Hosts([]string{"rec-ap-singapore-1.byteplusapi.com"}).
		//Headers(map[string]string{"Customer-Header": "Value"}). // Optional
		Build()
	concurrentHelper = NewConcurrentHelper(client)
}

/**
 * Those examples request server with account named 'media_demo',
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

	// Write real-time content data˚
	writeContentsExample()
	// Write real-time content data concurrently
	concurrentWriteContentsExample()

	// Write real-time user event data
	writeUserEventsExample()
	// Write real-time user event data concurrently
	concurrentWriteUserEventsExample()

	// Pass a date list to mark the completion of data synchronization for these days.
	doneExample()

	// Pause for 5 seconds until the asynchronous import task completes
	time.Sleep(5 * time.Second)
	client.Release()
	os.Exit(0)
}

func writeUsersExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteUsersRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	response, err := client.WriteUsers(request, opts...)
	if err != nil {
		logs.Error("write user occur err, msg:%s", err.Error())
		return
	}
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

func buildWriteUsersRequest(count int) *protocol.WriteUsersRequest {
	users := mockUsers(count)
	return &protocol.WriteUsersRequest{
		Users: users,
		// Extra: map[string]string{"extra_info": "extra"},
	}
}

func writeContentsExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteContentsRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	response, err := client.WriteContents(request, opts...)
	if err != nil {
		logs.Error("write content occur err, msg:%s", err.Error())
		return
	}
	if common.IsUploadSuccess(response.GetStatus()) {
		logs.Info("write content success")
		return
	}
	logs.Error("write content find failure info, msg:%s errItems:%+v",
		response.GetStatus(), response.GetErrors())
}

func concurrentWriteContentsExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteContentsRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	_ = concurrentHelper.SubmitRequest(request, opts...)
}

func buildWriteContentsRequest(count int) *protocol.WriteContentsRequest {
	contents := mockContents(count)
	return &protocol.WriteContentsRequest{
		Contents: contents,
		// Extra:    map[string]string{"extra_info": "extra"},
	}
}

func writeUserEventsExample() {
	// The "WriteXXX" api can transfer max to 2000 items at one request
	request := buildWriteUserEventsRequest(1)
	opts := defaultOptions(DefaultWriteTimeout)
	response, err := client.WriteUserEvents(request, opts...)
	if err != nil {
		logs.Error("write user event occur err, msg:%s", err.Error())
		return
	}
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

func buildWriteUserEventsRequest(count int) *protocol.WriteUserEventsRequest {
	userEvents := mockUserEvents(count)
	return &protocol.WriteUserEventsRequest{
		UserEvents: userEvents,
		// Extra:      map[string]string{"extra_info": "extra"},
	}
}

func doneExample() {
	date, _ := time.Parse("20060102", "20210908")
	dateList := []time.Time{date}
	opts := defaultOptions(DefaultDoneTimeout)
	response, err := client.Done(dateList, TopicUser, opts...)
	if err != nil {
		logs.Error("[Done] occur error, msg:%s", err.Error())
		return
	}
	if common.IsSuccess(response.GetStatus()) {
		logs.Info("[Done] success")
		return
	}
	logs.Error("[Done] find failure info, rsp:%s", response)
}

func defaultOptions(timeout time.Duration) []option.Option {
	// All options are optional
	// var customerHeaders map[string]string
	opts := []option.Option{
		option.WithRequestId(uuid.NewString()),
		option.WithTimeout(timeout),
		//option.WithHeaders(customerHeaders),
	}
	return opts
}

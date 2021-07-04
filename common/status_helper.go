package common

import . "github.com/byteplus-sdk/sdk-go/common/protocol"
import . "github.com/byteplus-sdk/sdk-go/core"

func IsUploadSuccess(status *Status) bool {
	code := status.Code
	// It is still considered as success, which is rejected for idempotent
	return code == StatusCodeSuccess || code == StatusCodeIdempotent
}

func IsSuccess(status *Status) bool {
	code := status.Code
	return code == StatusCodeSuccess
}

func IsSuccessCode(code int32) bool {
	return code == StatusCodeSuccess
}

func IsServerOverload(status *Status) bool {
	code := status.Code
	return code == StatusCodeTooManyRequest
}

func IsLossOperation(status *Status) bool {
	code := status.Code
	return code == StatusCodeOperationLoss
}

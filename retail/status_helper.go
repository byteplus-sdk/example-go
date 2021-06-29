package main

import . "github.com/byteplus-sdk/sdk-go/retail/protocol"
import . "github.com/byteplus-sdk/sdk-go/core"

func isUploadSuccess(status *Status) bool {
	code := status.Code
	// It is still considered as success, which is rejected for idempotent
	return code == StatusCodeSuccess || code == StatusCodeIdempotent
}

func isSuccess(status *Status) bool {
	code := status.Code
	return code == StatusCodeSuccess
}

func isServerOverload(status *Status) bool {
	code := status.Code
	return code == StatusCodeTooManyRequest
}

func isLossOperation(status *Status) bool {
	code := status.Code
	return code == StatusCodeOperationLoss
}

/*
Copyright 2018 Sgoby.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vterrors

import (
	vtrpcpb "github.com/sgoby/sqlparser/vt/proto/vtrpc"
)

// This file contains the necessary methods to send and receive errors
// as payloads of proto3 structures. It converts vtError to and from
// *vtrpcpb.RPCError. Use these methods when a RPC call can return both
// data and an error.

// FromVTRPC recovers a vtError from a *vtrpcpb.RPCError (which is how vtError
// is transmitted across proto3 RPC boundaries).
func FromVTRPC(rpcErr *vtrpcpb.RPCError) error {
	if rpcErr == nil {
		return nil
	}
	code := rpcErr.Code
	if code == vtrpcpb.Code_OK {
		code = LegacyErrorCodeToCode(rpcErr.LegacyCode)
	}
	return New(code, rpcErr.Message)
}

// ToVTRPC converts from vtError to a vtrpcpb.RPCError.
func ToVTRPC(err error) *vtrpcpb.RPCError {
	if err == nil {
		return nil
	}
	code := Code(err)
	return &vtrpcpb.RPCError{
		LegacyCode: CodeToLegacyErrorCode(code),
		Code:       code,
		Message:    err.Error(),
	}
}

// CodeToLegacyErrorCode maps a vtrpcpb.Code to a vtrpcpb.LegacyErrorCode.
func CodeToLegacyErrorCode(code vtrpcpb.Code) vtrpcpb.LegacyErrorCode {
	switch code {
	case vtrpcpb.Code_OK:
		return vtrpcpb.LegacyErrorCode_SUCCESS_LEGACY
	case vtrpcpb.Code_CANCELED:
		return vtrpcpb.LegacyErrorCode_CANCELLED_LEGACY
	case vtrpcpb.Code_UNKNOWN:
		return vtrpcpb.LegacyErrorCode_UNKNOWN_ERROR_LEGACY
	case vtrpcpb.Code_INVALID_ARGUMENT:
		return vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY
	case vtrpcpb.Code_DEADLINE_EXCEEDED:
		return vtrpcpb.LegacyErrorCode_DEADLINE_EXCEEDED_LEGACY
	case vtrpcpb.Code_ALREADY_EXISTS:
		return vtrpcpb.LegacyErrorCode_INTEGRITY_ERROR_LEGACY
	case vtrpcpb.Code_PERMISSION_DENIED:
		return vtrpcpb.LegacyErrorCode_PERMISSION_DENIED_LEGACY
	case vtrpcpb.Code_RESOURCE_EXHAUSTED:
		return vtrpcpb.LegacyErrorCode_RESOURCE_EXHAUSTED_LEGACY
	case vtrpcpb.Code_FAILED_PRECONDITION:
		return vtrpcpb.LegacyErrorCode_QUERY_NOT_SERVED_LEGACY
	case vtrpcpb.Code_ABORTED:
		return vtrpcpb.LegacyErrorCode_NOT_IN_TX_LEGACY
	case vtrpcpb.Code_INTERNAL:
		return vtrpcpb.LegacyErrorCode_INTERNAL_ERROR_LEGACY
	case vtrpcpb.Code_UNAVAILABLE:
		// Legacy code assumes Unavailable errors are sent as Internal.
		return vtrpcpb.LegacyErrorCode_INTERNAL_ERROR_LEGACY
	case vtrpcpb.Code_UNAUTHENTICATED:
		return vtrpcpb.LegacyErrorCode_UNAUTHENTICATED_LEGACY
	default:
		return vtrpcpb.LegacyErrorCode_UNKNOWN_ERROR_LEGACY
	}
}


// LegacyErrorCodeToCode maps a vtrpcpb.LegacyErrorCode to a gRPC vtrpcpb.Code.
func LegacyErrorCodeToCode(code vtrpcpb.LegacyErrorCode) vtrpcpb.Code {
	switch code {
	case vtrpcpb.LegacyErrorCode_SUCCESS_LEGACY:
		return vtrpcpb.Code_OK
	case vtrpcpb.LegacyErrorCode_CANCELLED_LEGACY:
		return vtrpcpb.Code_CANCELED
	case vtrpcpb.LegacyErrorCode_UNKNOWN_ERROR_LEGACY:
		return vtrpcpb.Code_UNKNOWN
	case vtrpcpb.LegacyErrorCode_BAD_INPUT_LEGACY:
		return vtrpcpb.Code_INVALID_ARGUMENT
	case vtrpcpb.LegacyErrorCode_DEADLINE_EXCEEDED_LEGACY:
		return vtrpcpb.Code_DEADLINE_EXCEEDED
	case vtrpcpb.LegacyErrorCode_INTEGRITY_ERROR_LEGACY:
		return vtrpcpb.Code_ALREADY_EXISTS
	case vtrpcpb.LegacyErrorCode_PERMISSION_DENIED_LEGACY:
		return vtrpcpb.Code_PERMISSION_DENIED
	case vtrpcpb.LegacyErrorCode_RESOURCE_EXHAUSTED_LEGACY:
		return vtrpcpb.Code_RESOURCE_EXHAUSTED
	case vtrpcpb.LegacyErrorCode_QUERY_NOT_SERVED_LEGACY:
		return vtrpcpb.Code_FAILED_PRECONDITION
	case vtrpcpb.LegacyErrorCode_NOT_IN_TX_LEGACY:
		return vtrpcpb.Code_ABORTED
	case vtrpcpb.LegacyErrorCode_INTERNAL_ERROR_LEGACY:
		// Legacy code sends internal error instead of Unavailable.
		return vtrpcpb.Code_UNAVAILABLE
	case vtrpcpb.LegacyErrorCode_TRANSIENT_ERROR_LEGACY:
		return vtrpcpb.Code_UNAVAILABLE
	case vtrpcpb.LegacyErrorCode_UNAUTHENTICATED_LEGACY:
		return vtrpcpb.Code_UNAUTHENTICATED
	default:
		return vtrpcpb.Code_UNKNOWN
	}
}
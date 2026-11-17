package s3liteobjectstore

import (
	"encoding/xml"
	"fmt"
	"net/http"
)

// S3 error code constants returned by the server and storage backends.
const (
	ErrCodeAccessDenied                 = "AccessDenied"
	ErrCodeSignatureDoesNotMatch        = "SignatureDoesNotMatch"
	ErrCodeRequestTimeTooSkewed         = "RequestTimeTooSkewed"
	ErrCodeInvalidAccessKeyId           = "InvalidAccessKeyId"
	ErrCodeAuthorizationHeaderMalformed = "AuthorizationHeaderMalformed"
	ErrCodeXAmzContentSHA256Mismatch    = "XAmzContentSHA256Mismatch"
	ErrCodeInvalidBucketName            = "InvalidBucketName"
	ErrCodeBucketAlreadyExists          = "BucketAlreadyExists"
	ErrCodeBucketAlreadyOwnedByYou      = "BucketAlreadyOwnedByYou"
	ErrCodeNoSuchBucket                 = "NoSuchBucket"
	ErrCodeBucketNotEmpty               = "BucketNotEmpty"
	ErrCodeNoSuchKey                    = "NoSuchKey"
	ErrCodeKeyTooLongError              = "KeyTooLongError"
	ErrCodeEntityTooLarge               = "EntityTooLarge"
	ErrCodeEntityTooSmall               = "EntityTooSmall"
	ErrCodeInvalidRange                 = "InvalidRange"
	ErrCodeNoSuchUpload                 = "NoSuchUpload"
	ErrCodeInvalidPart                  = "InvalidPart"
	ErrCodeInvalidPartOrder             = "InvalidPartOrder"
	ErrCodeInvalidArgument              = "InvalidArgument"
	ErrCodeMalformedXML                 = "MalformedXML"
	ErrCodeNotImplemented               = "NotImplemented"
	ErrCodeServiceUnavailable           = "ServiceUnavailable"
	ErrCodeInternalError                = "InternalError"
)

// Error represents a typed S3 error with a stable machine-readable Code and HTTP Status.
type Error struct {
	Code    string
	Message string
	Status  int
}

// Error implements the standard error interface.
func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	msg := e.Message
	if len(msg) > 200 {
		msg = msg[:200]
	}
	if msg == "" {
		return fmt.Sprintf("s3liteobjectstore: %s (status %d)", e.Code, e.Status)
	}
	return fmt.Sprintf("s3liteobjectstore: %s: %s", e.Code, msg)
}

// Is reports whether target is an [*Error] with the same Code.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	if !ok || e == nil || t == nil {
		return false
	}
	return e.Code == t.Code
}

// Predefined sentinel errors for use with [errors.Is].
var (
	ErrAccessDenied                 = &Error{Code: ErrCodeAccessDenied, Message: "Access Denied.", Status: http.StatusForbidden}
	ErrSignatureDoesNotMatch        = &Error{Code: ErrCodeSignatureDoesNotMatch, Message: "The request signature we calculated does not match the signature you provided.", Status: http.StatusForbidden}
	ErrRequestTimeTooSkewed         = &Error{Code: ErrCodeRequestTimeTooSkewed, Message: "The difference between the request time and the current time is too large.", Status: http.StatusForbidden}
	ErrInvalidAccessKeyId           = &Error{Code: ErrCodeInvalidAccessKeyId, Message: "The AWS Access Key Id you provided does not exist in our records.", Status: http.StatusForbidden}
	ErrAuthorizationHeaderMalformed = &Error{Code: ErrCodeAuthorizationHeaderMalformed, Message: "The authorization header is malformed.", Status: http.StatusBadRequest}
	ErrXAmzContentSHA256Mismatch    = &Error{Code: ErrCodeXAmzContentSHA256Mismatch, Message: "The provided 'x-amz-content-sha256' header does not match what was computed.", Status: http.StatusBadRequest}
	ErrInvalidBucketName            = &Error{Code: ErrCodeInvalidBucketName, Message: "The specified bucket is not valid.", Status: http.StatusBadRequest}
	ErrBucketAlreadyOwnedByYou      = &Error{Code: ErrCodeBucketAlreadyOwnedByYou, Message: "Your previous request to create the named bucket succeeded and you already own it.", Status: http.StatusConflict}
	ErrNoSuchBucket                 = &Error{Code: ErrCodeNoSuchBucket, Message: "The specified bucket does not exist.", Status: http.StatusNotFound}
	ErrBucketNotEmpty               = &Error{Code: ErrCodeBucketNotEmpty, Message: "The bucket you tried to delete is not empty.", Status: http.StatusConflict}
	ErrNoSuchKey                    = &Error{Code: ErrCodeNoSuchKey, Message: "The specified key does not exist.", Status: http.StatusNotFound}
	ErrKeyTooLongError              = &Error{Code: ErrCodeKeyTooLongError, Message: "Your key is too long.", Status: http.StatusBadRequest}
	ErrEntityTooLarge               = &Error{Code: ErrCodeEntityTooLarge, Message: "Your proposed upload exceeds the maximum allowed object size.", Status: http.StatusBadRequest}
	ErrEntityTooSmall               = &Error{Code: ErrCodeEntityTooSmall, Message: "Your proposed upload is smaller than the minimum allowed object size.", Status: http.StatusBadRequest}
	ErrInvalidRange                 = &Error{Code: ErrCodeInvalidRange, Message: "The requested range cannot be satisfied.", Status: http.StatusRequestedRangeNotSatisfiable}
	ErrNoSuchUpload                 = &Error{Code: ErrCodeNoSuchUpload, Message: "The specified upload does not exist.", Status: http.StatusNotFound}
	ErrInvalidPart                  = &Error{Code: ErrCodeInvalidPart, Message: "One or more of the specified parts could not be found or the specified entity tag might not have matched the part's entity tag.", Status: http.StatusBadRequest}
	ErrInvalidPartOrder             = &Error{Code: ErrCodeInvalidPartOrder, Message: "The list of parts was not in ascending order.", Status: http.StatusBadRequest}
	ErrInvalidArgument              = &Error{Code: ErrCodeInvalidArgument, Message: "Invalid Argument.", Status: http.StatusBadRequest}
	ErrMalformedXML                 = &Error{Code: ErrCodeMalformedXML, Message: "The XML you provided was not well-formed or did not validate against our published schema.", Status: http.StatusBadRequest}
	ErrNotImplemented               = &Error{Code: ErrCodeNotImplemented, Message: "A header or query you provided implies functionality that is not implemented.", Status: http.StatusNotImplemented}
	ErrServiceUnavailable           = &Error{Code: ErrCodeServiceUnavailable, Message: "Service unavailable.", Status: http.StatusServiceUnavailable}
	ErrInternalError                = &Error{Code: ErrCodeInternalError, Message: "We encountered an internal error. Please try again.", Status: http.StatusInternalServerError}
)

func newError(base *Error, msg string) *Error {
	if len(msg) > 200 {
		msg = msg[:200]
	}
	return &Error{
		Code:    base.Code,
		Message: msg,
		Status:  base.Status,
	}
}

type xmlErrorResponse struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource"`
	RequestID string   `xml:"RequestId"`
}

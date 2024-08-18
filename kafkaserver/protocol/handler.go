// Package protocol - This is a generated file, please do not edit
package protocol

import (
    "encoding/binary"
    "fmt"
    "github.com/pkg/errors"
    "net"
)

func HandleRequestBuffer(apiKey int16, buff []byte, handler RequestHandler, conn net.Conn) error {
    apiVersion := int16(binary.BigEndian.Uint16(buff[2:]))
    var err error
    var responseHeader ResponseHeader
    switch apiKey {
    case 0:
        var req ProduceRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *ProduceResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.ProduceRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleProduceRequest(&requestHeader, &req, respFunc)   
        }
    case 1:
        var req FetchRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *FetchResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.FetchRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleFetchRequest(&requestHeader, &req, respFunc)   
        }
    case 2:
        var req ListOffsetsRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *ListOffsetsResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.ListOffsetsRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleListOffsetsRequest(&requestHeader, &req, respFunc)   
        }
    case 3:
        var req MetadataRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *MetadataResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.MetadataRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleMetadataRequest(&requestHeader, &req, respFunc)   
        }
    case 8:
        var req OffsetCommitRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *OffsetCommitResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.OffsetCommitRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleOffsetCommitRequest(&requestHeader, &req, respFunc)   
        }
    case 9:
        var req OffsetFetchRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *OffsetFetchResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.OffsetFetchRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleOffsetFetchRequest(&requestHeader, &req, respFunc)   
        }
    case 10:
        var req FindCoordinatorRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *FindCoordinatorResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.FindCoordinatorRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleFindCoordinatorRequest(&requestHeader, &req, respFunc)   
        }
    case 11:
        var req JoinGroupRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *JoinGroupResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.JoinGroupRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleJoinGroupRequest(&requestHeader, &req, respFunc)   
        }
    case 12:
        var req HeartbeatRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *HeartbeatResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.HeartbeatRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleHeartbeatRequest(&requestHeader, &req, respFunc)   
        }
    case 13:
        var req LeaveGroupRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *LeaveGroupResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.LeaveGroupRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleLeaveGroupRequest(&requestHeader, &req, respFunc)   
        }
    case 14:
        var req SyncGroupRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *SyncGroupResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.SyncGroupRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleSyncGroupRequest(&requestHeader, &req, respFunc)   
        }
    case 18:
        var req ApiVersionsRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *ApiVersionsResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.ApiVersionsRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleApiVersionsRequest(&requestHeader, &req, respFunc)   
        }
    case 36:
        var req SaslAuthenticateRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *SaslAuthenticateResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.SaslAuthenticateRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleSaslAuthenticateRequest(&requestHeader, &req, respFunc)   
        }
    case 17:
        var req SaslHandshakeRequest
        requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
        var requestHeader RequestHeader
        var offset int
        if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
		    return err
	    }
        responseHeader.CorrelationId = requestHeader.CorrelationId
        if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
            return err
        }
        respFunc := func(resp *SaslHandshakeResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        }
 		minVer, maxVer := req.SupportedApiVersions()
        if apiVersion < minVer || apiVersion > maxVer {
			resp := handler.SaslHandshakeRequestErrorResponse(ErrorCodeUnsupportedVersion, fmt.Sprintf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer), &req)
            err = respFunc(resp)
		} else {
            err = handler.HandleSaslHandshakeRequest(&requestHeader, &req, respFunc)   
        }
    default: return errors.Errorf("Unsupported ApiKey: %d", apiKey)
    }
    return err
}

type RequestHandler interface {
    HandleProduceRequest(hdr *RequestHeader, req *ProduceRequest, completionFunc func(resp *ProduceResponse) error) error
    ProduceRequestErrorResponse(errorCode int16, errorMsg string, req *ProduceRequest) *ProduceResponse
    HandleFetchRequest(hdr *RequestHeader, req *FetchRequest, completionFunc func(resp *FetchResponse) error) error
    FetchRequestErrorResponse(errorCode int16, errorMsg string, req *FetchRequest) *FetchResponse
    HandleListOffsetsRequest(hdr *RequestHeader, req *ListOffsetsRequest, completionFunc func(resp *ListOffsetsResponse) error) error
    ListOffsetsRequestErrorResponse(errorCode int16, errorMsg string, req *ListOffsetsRequest) *ListOffsetsResponse
    HandleMetadataRequest(hdr *RequestHeader, req *MetadataRequest, completionFunc func(resp *MetadataResponse) error) error
    MetadataRequestErrorResponse(errorCode int16, errorMsg string, req *MetadataRequest) *MetadataResponse
    HandleOffsetCommitRequest(hdr *RequestHeader, req *OffsetCommitRequest, completionFunc func(resp *OffsetCommitResponse) error) error
    OffsetCommitRequestErrorResponse(errorCode int16, errorMsg string, req *OffsetCommitRequest) *OffsetCommitResponse
    HandleOffsetFetchRequest(hdr *RequestHeader, req *OffsetFetchRequest, completionFunc func(resp *OffsetFetchResponse) error) error
    OffsetFetchRequestErrorResponse(errorCode int16, errorMsg string, req *OffsetFetchRequest) *OffsetFetchResponse
    HandleFindCoordinatorRequest(hdr *RequestHeader, req *FindCoordinatorRequest, completionFunc func(resp *FindCoordinatorResponse) error) error
    FindCoordinatorRequestErrorResponse(errorCode int16, errorMsg string, req *FindCoordinatorRequest) *FindCoordinatorResponse
    HandleJoinGroupRequest(hdr *RequestHeader, req *JoinGroupRequest, completionFunc func(resp *JoinGroupResponse) error) error
    JoinGroupRequestErrorResponse(errorCode int16, errorMsg string, req *JoinGroupRequest) *JoinGroupResponse
    HandleHeartbeatRequest(hdr *RequestHeader, req *HeartbeatRequest, completionFunc func(resp *HeartbeatResponse) error) error
    HeartbeatRequestErrorResponse(errorCode int16, errorMsg string, req *HeartbeatRequest) *HeartbeatResponse
    HandleLeaveGroupRequest(hdr *RequestHeader, req *LeaveGroupRequest, completionFunc func(resp *LeaveGroupResponse) error) error
    LeaveGroupRequestErrorResponse(errorCode int16, errorMsg string, req *LeaveGroupRequest) *LeaveGroupResponse
    HandleSyncGroupRequest(hdr *RequestHeader, req *SyncGroupRequest, completionFunc func(resp *SyncGroupResponse) error) error
    SyncGroupRequestErrorResponse(errorCode int16, errorMsg string, req *SyncGroupRequest) *SyncGroupResponse
    HandleApiVersionsRequest(hdr *RequestHeader, req *ApiVersionsRequest, completionFunc func(resp *ApiVersionsResponse) error) error
    ApiVersionsRequestErrorResponse(errorCode int16, errorMsg string, req *ApiVersionsRequest) *ApiVersionsResponse
    HandleSaslAuthenticateRequest(hdr *RequestHeader, req *SaslAuthenticateRequest, completionFunc func(resp *SaslAuthenticateResponse) error) error
    SaslAuthenticateRequestErrorResponse(errorCode int16, errorMsg string, req *SaslAuthenticateRequest) *SaslAuthenticateResponse
    HandleSaslHandshakeRequest(hdr *RequestHeader, req *SaslHandshakeRequest, completionFunc func(resp *SaslHandshakeResponse) error) error
    SaslHandshakeRequestErrorResponse(errorCode int16, errorMsg string, req *SaslHandshakeRequest) *SaslHandshakeResponse
}

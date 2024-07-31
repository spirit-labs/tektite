// Package protocol - This is a generated file, please do not edit
package protocol

import (
    "encoding/binary"
    "github.com/spirit-labs/tektite/errors"
    "net"
)

func HandleRequestBuffer(buff []byte, handler RequestHandler, conn net.Conn) error {
    apiKey := int16(binary.BigEndian.Uint16(buff))
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
		err = handler.HandleProduceRequest(&requestHeader, &req, func(resp *ProduceResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleFetchRequest(&requestHeader, &req, func(resp *FetchResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleListOffsetsRequest(&requestHeader, &req, func(resp *ListOffsetsResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleMetadataRequest(&requestHeader, &req, func(resp *MetadataResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleOffsetCommitRequest(&requestHeader, &req, func(resp *OffsetCommitResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleOffsetFetchRequest(&requestHeader, &req, func(resp *OffsetFetchResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleFindCoordinatorRequest(&requestHeader, &req, func(resp *FindCoordinatorResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleJoinGroupRequest(&requestHeader, &req, func(resp *JoinGroupResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleHeartbeatRequest(&requestHeader, &req, func(resp *HeartbeatResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleLeaveGroupRequest(&requestHeader, &req, func(resp *LeaveGroupResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleSyncGroupRequest(&requestHeader, &req, func(resp *SyncGroupResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
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
		err = handler.HandleApiVersionsRequest(&requestHeader, &req, func(resp *ApiVersionsResponse) error {
            respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
            respSize, tagSizes := resp.CalcSize(apiVersion, nil)
            totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4 + totRespSize)
            respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
            respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
            respBuff = resp.Write(apiVersion, respBuff, tagSizes)
            _, err := conn.Write(respBuff)
            return err
        })
    default: return errors.Errorf("Unknown ApiKey: %d", apiKey)
    }
    return err
}

type RequestHandler interface {
    HandleProduceRequest(hdr *RequestHeader, req *ProduceRequest, completionFunc func(resp *ProduceResponse) error) error
    HandleFetchRequest(hdr *RequestHeader, req *FetchRequest, completionFunc func(resp *FetchResponse) error) error
    HandleListOffsetsRequest(hdr *RequestHeader, req *ListOffsetsRequest, completionFunc func(resp *ListOffsetsResponse) error) error
    HandleMetadataRequest(hdr *RequestHeader, req *MetadataRequest, completionFunc func(resp *MetadataResponse) error) error
    HandleOffsetCommitRequest(hdr *RequestHeader, req *OffsetCommitRequest, completionFunc func(resp *OffsetCommitResponse) error) error
    HandleOffsetFetchRequest(hdr *RequestHeader, req *OffsetFetchRequest, completionFunc func(resp *OffsetFetchResponse) error) error
    HandleFindCoordinatorRequest(hdr *RequestHeader, req *FindCoordinatorRequest, completionFunc func(resp *FindCoordinatorResponse) error) error
    HandleJoinGroupRequest(hdr *RequestHeader, req *JoinGroupRequest, completionFunc func(resp *JoinGroupResponse) error) error
    HandleHeartbeatRequest(hdr *RequestHeader, req *HeartbeatRequest, completionFunc func(resp *HeartbeatResponse) error) error
    HandleLeaveGroupRequest(hdr *RequestHeader, req *LeaveGroupRequest, completionFunc func(resp *LeaveGroupResponse) error) error
    HandleSyncGroupRequest(hdr *RequestHeader, req *SyncGroupRequest, completionFunc func(resp *SyncGroupResponse) error) error
    HandleApiVersionsRequest(hdr *RequestHeader, req *ApiVersionsRequest, completionFunc func(resp *ApiVersionsResponse) error) error
}

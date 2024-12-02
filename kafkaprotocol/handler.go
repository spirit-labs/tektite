// Package kafkaprotocol - This is a generated file, please do not edit
package kafkaprotocol

import (
    "encoding/binary"
    "github.com/pkg/errors"
    "net"
)

func checkSupportedVersion(apiKey int16, apiVersion int16, minVer int16, maxVer int16) error {
	if apiVersion < minVer || apiVersion > maxVer {
        // connection will be closed if version is not supported
		return errors.Errorf("version %d for apiKey %d is unsupported. supported versions are %d to %d", apiVersion, apiKey, minVer, maxVer)
	}
	return nil
}

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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleProduceRequest(&requestHeader, &req, func(resp *ProduceResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleFetchRequest(&requestHeader, &req, func(resp *FetchResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleListOffsetsRequest(&requestHeader, &req, func(resp *ListOffsetsResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleMetadataRequest(&requestHeader, &req, func(resp *MetadataResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleOffsetCommitRequest(&requestHeader, &req, func(resp *OffsetCommitResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleOffsetFetchRequest(&requestHeader, &req, func(resp *OffsetFetchResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleFindCoordinatorRequest(&requestHeader, &req, func(resp *FindCoordinatorResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleJoinGroupRequest(&requestHeader, &req, func(resp *JoinGroupResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleHeartbeatRequest(&requestHeader, &req, func(resp *HeartbeatResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleLeaveGroupRequest(&requestHeader, &req, func(resp *LeaveGroupResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleSyncGroupRequest(&requestHeader, &req, func(resp *SyncGroupResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
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
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleApiVersionsRequest(&requestHeader, &req, func(resp *ApiVersionsResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 22:
		var req InitProducerIdRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleInitProducerIdRequest(&requestHeader, &req, func(resp *InitProducerIdResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 36:
		var req SaslAuthenticateRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleSaslAuthenticateRequest(&requestHeader, &req, func(resp *SaslAuthenticateResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 17:
		var req SaslHandshakeRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleSaslHandshakeRequest(&requestHeader, &req, func(resp *SaslHandshakeResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 25:
		var req AddOffsetsToTxnRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleAddOffsetsToTxnRequest(&requestHeader, &req, func(resp *AddOffsetsToTxnResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 24:
		var req AddPartitionsToTxnRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleAddPartitionsToTxnRequest(&requestHeader, &req, func(resp *AddPartitionsToTxnResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 28:
		var req TxnOffsetCommitRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleTxnOffsetCommitRequest(&requestHeader, &req, func(resp *TxnOffsetCommitResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 26:
		var req EndTxnRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleEndTxnRequest(&requestHeader, &req, func(resp *EndTxnResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 1000:
		var req PutUserCredentialsRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandlePutUserCredentialsRequest(&requestHeader, &req, func(resp *PutUserCredentialsResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    case 1001:
		var req DeleteUserRequest
		requestHeaderVersion, responseHeaderVersion := req.HeaderVersions(apiVersion)
		var requestHeader RequestHeader
		var offset int
		if offset, err = requestHeader.Read(requestHeaderVersion, buff); err != nil {
			return err
		}
		minVer, maxVer := req.SupportedApiVersions()
		if err := checkSupportedVersion(apiKey, apiVersion, minVer, maxVer); err != nil {
			return err
		}
		if _, err := req.Read(apiVersion, buff[offset:]); err != nil {
			return err
		}
		responseHeader.CorrelationId = requestHeader.CorrelationId
		err = handler.HandleDeleteUserRequest(&requestHeader, &req, func(resp *DeleteUserResponse) error {
			respHeaderSize, hdrTagSizes := responseHeader.CalcSize(responseHeaderVersion, nil)
			respSize, tagSizes := resp.CalcSize(apiVersion, nil)
			totRespSize := respHeaderSize + respSize
			respBuff := make([]byte, 0, 4+totRespSize)
			respBuff = binary.BigEndian.AppendUint32(respBuff, uint32(totRespSize))
			respBuff = responseHeader.Write(responseHeaderVersion, respBuff, hdrTagSizes)
			respBuff = resp.Write(apiVersion, respBuff, tagSizes)
			_, err := conn.Write(respBuff)
			return err
		})
    default: return errors.Errorf("Unsupported ApiKey: %d", apiKey)
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
    HandleInitProducerIdRequest(hdr *RequestHeader, req *InitProducerIdRequest, completionFunc func(resp *InitProducerIdResponse) error) error
    HandleSaslAuthenticateRequest(hdr *RequestHeader, req *SaslAuthenticateRequest, completionFunc func(resp *SaslAuthenticateResponse) error) error
    HandleSaslHandshakeRequest(hdr *RequestHeader, req *SaslHandshakeRequest, completionFunc func(resp *SaslHandshakeResponse) error) error
    HandleAddOffsetsToTxnRequest(hdr *RequestHeader, req *AddOffsetsToTxnRequest, completionFunc func(resp *AddOffsetsToTxnResponse) error) error
    HandleAddPartitionsToTxnRequest(hdr *RequestHeader, req *AddPartitionsToTxnRequest, completionFunc func(resp *AddPartitionsToTxnResponse) error) error
    HandleTxnOffsetCommitRequest(hdr *RequestHeader, req *TxnOffsetCommitRequest, completionFunc func(resp *TxnOffsetCommitResponse) error) error
    HandleEndTxnRequest(hdr *RequestHeader, req *EndTxnRequest, completionFunc func(resp *EndTxnResponse) error) error
    HandlePutUserCredentialsRequest(hdr *RequestHeader, req *PutUserCredentialsRequest, completionFunc func(resp *PutUserCredentialsResponse) error) error
    HandleDeleteUserRequest(hdr *RequestHeader, req *DeleteUserRequest, completionFunc func(resp *DeleteUserResponse) error) error
}

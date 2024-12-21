package agent

import (
	"github.com/spirit-labs/tektite/acls"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
)

var plain = auth.AuthenticationSaslPlain
var sha512 = auth.AuthenticationSaslScramSha512

func (k *kafkaHandler) HandlePutUserCredentialsRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.PutUserCredentialsRequest, completionFunc func(resp *kafkaprotocol.PutUserCredentialsResponse) error) error {
	var resp kafkaprotocol.PutUserCredentialsResponse
	errCode, errMsg := authoriseCluster(k.authContext, acls.OperationAlter, "not authorised to create/update user credentials")
	if errCode != kafkaprotocol.ErrorCodeNone {
		resp.ErrorCode = int16(errCode)
		resp.ErrorMessage = common.StrPtr(errMsg)
		return completionFunc(&resp)
	}
	cl, err := k.agent.controlClientCache.GetClient()
	setErrorForPutUserResponse(err, &resp)
	if err == nil {
		username := common.SafeDerefStringPtr(req.Username)
		salt := common.SafeDerefStringPtr(req.Salt)
		err = cl.PutUserCredentials(username, req.StoredKey, req.ServerKey, salt, int(req.Iters))
		setErrorForPutUserResponse(err, &resp)
	}
	return completionFunc(&resp)
}

func setErrorForPutUserResponse(err error, resp *kafkaprotocol.PutUserCredentialsResponse) {
	if err != nil {
		resp.ErrorMessage = common.StrPtr(err.Error())
		if common.IsUnavailableError(err) {
			resp.ErrorCode = kafkaprotocol.ErrorCodeCoordinatorNotAvailable
		} else {
			resp.ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
		}
	}
}

func (k *kafkaHandler) HandleDeleteUserRequest(_ *kafkaprotocol.RequestHeader, req *kafkaprotocol.DeleteUserRequest,
	completionFunc func(resp *kafkaprotocol.DeleteUserResponse) error) error {
	var resp kafkaprotocol.DeleteUserResponse
	errCode, errMsg := authoriseCluster(k.authContext, acls.OperationAlter, "not authorised to delete user credentials")
	if errCode != kafkaprotocol.ErrorCodeNone {
		resp.ErrorCode = int16(errCode)
		resp.ErrorMessage = common.StrPtr(errMsg)
		return completionFunc(&resp)
	}
	cl, err := k.agent.controlClientCache.GetClient()
	setErrorForDeleteUserResponse(err, &resp)
	if err == nil {
		username := common.SafeDerefStringPtr(req.Username)
		err = cl.DeleteUserCredentials(username)
		setErrorForDeleteUserResponse(err, &resp)
	}
	return completionFunc(&resp)
}

func setErrorForDeleteUserResponse(err error, resp *kafkaprotocol.DeleteUserResponse) {
	if err != nil {
		resp.ErrorMessage = common.StrPtr(err.Error())
		if common.IsUnavailableError(err) {
			resp.ErrorCode = kafkaprotocol.ErrorCodeCoordinatorNotAvailable
		} else if common.IsTektiteErrorWithCode(err, common.NoSuchUser) {
			resp.ErrorCode = kafkaprotocol.ErrorCodeNoSuchUser
			resp.ErrorMessage = common.StrPtr(err.Error())
		} else {
			resp.ErrorCode = kafkaprotocol.ErrorCodeUnknownServerError
		}
	}
}


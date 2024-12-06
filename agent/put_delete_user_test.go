package agent

import (
	"encoding/base64"
	"fmt"
	"github.com/spirit-labs/tektite/apiclient"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestPutDeleteUser(t *testing.T) {
	cfg := NewConf()
	cfg.PusherConf.WriteTimeout = 1 * time.Millisecond // for fast commit of user creds
	agents, tearDown := setupAgents(t, cfg, 1, func(i int) string {
		return "az1"
	})
	defer tearDown(t)
	agent := agents[0]

	cl, err := apiclient.NewKafkaApiClientWithClientID("")
	require.NoError(t, err)
	conn, err := cl.NewConnection(agent.cfg.KafkaListenerConfig.Address)
	require.NoError(t, err)
	defer func() {
		err := conn.Close()
		require.NoError(t, err)
	}()

	numUsers := 10
	for i := 0; i < numUsers; i++ {
		username := fmt.Sprintf("test-user-%d", i)
		password := fmt.Sprintf("test-password-%d", i)
		storedKey, serverKey, salt := auth.CreateUserScramCreds(password, auth.AuthenticationSaslScramSha512)
		iters := 8192
		req := kafkaprotocol.PutUserCredentialsRequest{
			Username:  common.StrPtr(username),
			StoredKey: storedKey,
			ServerKey: serverKey,
			Salt:      common.StrPtr(salt),
			Iters:     int64(iters),
		}
		resp := &kafkaprotocol.PutUserCredentialsResponse{}
		r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyPutUserCredentialsRequest, 0, resp)
		require.NoError(t, err)
		resp = r.(*kafkaprotocol.PutUserCredentialsResponse)
		require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.ErrorCode))
		require.Nil(t, resp.ErrorMessage)

		creds, ok, err := agent.scramManager.LookupUserCreds(username)
		require.NoError(t, err)
		require.True(t, ok)

		decodedStoredKey, err := base64.StdEncoding.DecodeString(creds.StoredKey)
		require.NoError(t, err)
		decodedServerKey, err := base64.StdEncoding.DecodeString(creds.ServerKey)
		require.NoError(t, err)

		require.Equal(t, storedKey, decodedStoredKey)
		require.Equal(t, serverKey, decodedServerKey)
		require.Equal(t, salt, creds.Salt)
		require.Equal(t, iters, creds.Iters)
	}

	for i := 0; i < numUsers; i++ {
		username := fmt.Sprintf("test-user-%d", i)
		req := kafkaprotocol.DeleteUserRequest{
			Username: common.StrPtr(username),
		}
		resp := &kafkaprotocol.DeleteUserResponse{}
		r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyDeleteUserRequest, 0, resp)
		require.NoError(t, err)
		resp = r.(*kafkaprotocol.DeleteUserResponse)
		require.Equal(t, kafkaprotocol.ErrorCodeNone, int(resp.ErrorCode))
		require.Nil(t, resp.ErrorMessage)

		_, ok, err := agent.scramManager.LookupUserCreds(username)
		require.NoError(t, err)
		require.False(t, ok)
	}

	// Delete unknown user
	req := kafkaprotocol.DeleteUserRequest{
		Username: common.StrPtr("unknown"),
	}
	resp := &kafkaprotocol.DeleteUserResponse{}
	r, err := conn.SendRequest(&req, kafkaprotocol.ApiKeyDeleteUserRequest, 0, resp)
	require.NoError(t, err)
	resp = r.(*kafkaprotocol.DeleteUserResponse)
	require.Equal(t, kafkaprotocol.ErrorCodeNoSuchUser, int(resp.ErrorCode))
	require.Equal(t, "user does not exist", common.SafeDerefStringPtr(resp.ErrorMessage))
}

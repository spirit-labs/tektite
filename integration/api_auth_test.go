//go:build integration

package integration

import (
	"bytes"
	crand "crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"github.com/spirit-labs/tektite/asl/api"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/server"
	"github.com/spirit-labs/tektite/auth"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"io"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"
)

func sendApiRequest(t *testing.T, authType string, authHeader string, serverAddress string, cl *http.Client) (*http.Response, string, string) {
	topicName := fmt.Sprintf("my-topic-%s", uuid.New().String())
	stmt := fmt.Sprintf(`%s := (topic partitions = 16)`, topicName)
	resp, body := sendRequest(t, stmt, "statement", authType, authHeader, serverAddress, cl)
	return resp, body, topicName
}

func sendRequest(t *testing.T, reqBody string, endpoint string, authType string, authHeader string, serverAddress string, cl *http.Client) (*http.Response, string) {
	uri := fmt.Sprintf("https://%s/tektite/%s", serverAddress, endpoint)
	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBufferString(reqBody))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	if authType != "" {
		req.Header.Set("Authorization", authType+" "+authHeader)
	}
	resp, err := cl.Do(req)
	require.NoError(t, err)
	defer closeRespBody(t, resp)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp, string(body)
}

func TestApiAuth(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	servers, _ := startClusterFast(t, 3, objStore, func(cfg *conf.Config) {
		cfg.AuthenticationEnabled = true
	})
	cl := createHttpClient(t)
	defer cl.CloseIdleConnections()

	for _, tc := range apiAuthTestCases {
		t.Run(tc.caseName, func(t *testing.T) {
			tc.f(t, servers, cl)
		})
	}
}

type apiAuthTestCase struct {
	caseName string
	f        func(t *testing.T, servers []*server.Server, cl *http.Client)
}

var apiAuthTestCases = []apiAuthTestCase{
	{caseName: "testUnknownUser", f: testUnknownUserBasicAuth},
	{caseName: "testKnownUserBadPasswordBasicAuth", f: testKnownUserBadPasswordBasicAuth},
	{caseName: "testKnownUserAuthenticatedBasicAuthSameNode", f: testKnownUserAuthenticatedBasicAuthSameNode},
	{caseName: "testKnownUserAuthenticatedBasicAuthDifferentNode", f: testKnownUserAuthenticatedBasicAuthDifferentNode},
	{caseName: "testAuthWithSecondUsersPasswordBasicAuth", f: testAuthWithSecondUsersPasswordBasicAuth},
	{caseName: "testAuthWithJwtSameNode", f: testAuthWithJwtSameNode},
	{caseName: "testAuthWithJwtDifferentNode", f: testAuthWithJwtDifferentNode},
	{caseName: "testAuthWithJwtDifferentNodeServerBounced", f: testAuthWithJwtDifferentNodeServerBounced},
	{caseName: "testAuthWithMalformedJwt", f: testAuthWithMalformedJwt},
	{caseName: "testAuthWithWellFormedButInvalidJwt", f: testAuthWithWellFormedButInvalidJwt},
	{caseName: "testInvalidBasicAuthHeader", f: testInvalidBasicAuthHeader},
	{caseName: "testInvalidAuthType", f: testInvalidAuthType},
	{caseName: "testQueryEndpointAuthed", f: testQueryEndpointAuthed},
	{caseName: "testQueryEndpointNotAuthed", f: testQueryEndpointNotAuthed},
	{caseName: "testExecEndpointAuthed", f: testExecEndpointAuthed},
	{caseName: "testExecEndpointNotAuthed", f: testExecEndpointNotAuthed},
	{caseName: "testWasmRegisterAuthed", f: testWasmRegisterAuthed},
	{caseName: "testWasmRegisterNotAuthed", f: testWasmRegisterNotAuthed},
	{caseName: "testScramAuthDoesNotNeedAuth", f: testScramAuthDoesNotNeedAuth},
	{caseName: "testPubKeyDoesNotNeedAuth", f: testPubKeyDoesNotNeedAuth},
	{caseName: "testPutUserAuthed", f: testPutUserAuthed},
	{caseName: "testPutUserNotAuthed", f: testPutUserNotAuthed},
	{caseName: "testPutUser", f: testPutUser},
	{caseName: "testUpdateUserPasswordInvalidateJwt", f: testUpdateUserPasswordInvalidateJwt},
	{caseName: "testDeleteUserInvalidateJwt", f: testDeleteUserInvalidateJwt},
	{caseName: "testUpdateUserPasswordAuthRemainsCached", f: testUpdateUserPasswordAuthRemainsCached},
	{caseName: "testDeleteUser", f: testDeleteUser},
	{caseName: "testScramAuthUnknownConversationID", f: testScramAuthUnknownConversationID},
	{caseName: "testScramAuthInvalidAuthType", f: testScramAuthInvalidAuthType},
	{caseName: "testScramAuthWithClient", f: testScramAuthWithClient},
	{caseName: "testPutUserWithClient", f: testPutUserWithClient},
	{caseName: "testDeleteUserWithClient", f: testDeleteUserWithClient},
}

func TestApiAuthNotEnabled(t *testing.T) {
	objStore := dev.NewInMemStore(0)
	servers, _ := startClusterFast(t, 3, objStore, func(cfg *conf.Config) {
		cfg.AuthenticationEnabled = false
	})
	cl := createHttpClient(t)
	defer cl.CloseIdleConnections()

	for _, tc := range apiAuthNotEnabledCases {
		t.Run(tc.caseName, func(t *testing.T) {
			tc.f(t, servers, cl)
		})
	}
}

var apiAuthNotEnabledCases = []apiAuthTestCase{
	{caseName: "testQueryEndpointAuthNotNeeded", f: testQueryEndpointAuthNotNeeded},
	{caseName: "testExecEndpointAuthNotNeeded", f: testExecEndpointAuthNotNeeded},
	{caseName: "testWasmRegisterAuthNotNeeded", f: testWasmRegisterAuthNotNeeded},
	{caseName: "testScramAuthDoesNotNeedAuth", f: testScramAuthDoesNotNeedAuth},
	{caseName: "testPubKeyDoesNotNeedAuth", f: testPubKeyDoesNotNeedAuth},
	{caseName: "testPutUserAuthNotNeeded", f: testPutUserAuthNotNeeded},
}

func testUnknownUserBasicAuth(t *testing.T, servers []*server.Server, cl *http.Client) {
	resp, statusMessage, _ := sendApiRequest(t, "Basic", createBasicAuthHeader("unknownuser", "unknownpass"), randomServerAddress(servers), cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
	require.Empty(t, resp.Header.Get(api.JwtHeaderName))
}

func testKnownUserBadPasswordBasicAuth(t *testing.T, servers []*server.Server, cl *http.Client) {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)
	resp, statusMessage, _ := sendApiRequest(t, "Basic", createBasicAuthHeader(username, "aardvarks"), randomServerAddress(servers), cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
	require.Empty(t, resp.Header.Get(api.JwtHeaderName))
}

func testKnownUserAuthenticatedBasicAuthSameNode(t *testing.T, servers []*server.Server, cl *http.Client) {
	// create user and auth it on same node
	testKnownUserAuthenticatedBasicAuth(t, servers, cl, 1, 1)
}

func testKnownUserAuthenticatedBasicAuthDifferentNode(t *testing.T, servers []*server.Server, cl *http.Client) {
	// create user and auth it on different node
	testKnownUserAuthenticatedBasicAuth(t, servers, cl, 0, 2)
}

func testKnownUserAuthenticatedBasicAuth(t *testing.T, servers []*server.Server, cl *http.Client, createUserNode int,
	authNode int) {
	// create the user
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[createUserNode].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)
	// attempt auth
	resp, statusMessage, _ := sendApiRequest(t, "Basic", createBasicAuthHeader(username, password), servers[authNode].GetConfig().HttpApiAddresses[authNode], cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)
	// should be JWT in response
	tok := resp.Header.Get(api.JwtHeaderName)
	require.NotEmpty(t, tok)
}

func testAuthWithSecondUsersPasswordBasicAuth(t *testing.T, servers []*server.Server, cl *http.Client) {
	username1 := genUsername()
	password1 := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username1, password1, auth.AuthenticationSaslScramSha256)
	username2 := genUsername()
	password2 := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username2, password2, auth.AuthenticationSaslScramSha256)
	// attempt auth with other user's password
	resp, statusMessage, _ := sendApiRequest(t, "Basic", createBasicAuthHeader(username1, password2), servers[0].GetConfig().HttpApiAddresses[0], cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

func testAuthWithJwtSameNode(t *testing.T, servers []*server.Server, cl *http.Client) {
	tok, jwtCreateNode := performBasicAuthAndGetJwt(t, servers, cl)
	// now auth with the jwt on same node that the JWT was created on
	resp, statusMessage, _ := sendApiRequest(t, "Bearer", tok, servers[jwtCreateNode].GetConfig().HttpApiAddresses[jwtCreateNode], cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)
}

func testAuthWithJwtDifferentNode(t *testing.T, servers []*server.Server, cl *http.Client) {
	tok, jwtCreateNode := performBasicAuthAndGetJwt(t, servers, cl)
	// now auth with the jwt on a different node to where it was created, this will cache pub key info of the create node on
	// this node
	jwtAuthNode := (jwtCreateNode + 1) % len(servers)
	resp, statusMessage, _ := sendApiRequest(t, "Bearer", tok, servers[jwtAuthNode].GetConfig().HttpApiAddresses[jwtAuthNode], cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)
	// Now bounce the create node

	// Create a new user on the create node, and
}

func testAuthWithJwtDifferentNodeServerBounced(t *testing.T, servers []*server.Server, cl *http.Client) {
	// First create jwt on one node, this will encode the instance id of that node in the JWT
	jwtCreateNode := 0
	tok := performBasicAuthAndGetJwtOnNode(t, servers, cl, jwtCreateNode)
	// now auth with the jwt on a different node to where it was created
	jwtAuthNode := 1
	resp, statusMessage, _ := sendApiRequest(t, "Bearer", tok, servers[jwtAuthNode].GetConfig().HttpApiAddresses[jwtAuthNode], cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)

	// pub key should be cached on auth node
	createInstanceID := servers[jwtCreateNode].GetApiServer().GetInstanceID()
	infosMap := servers[jwtAuthNode].GetApiServer().GetCachedPubKeyInfos()
	info, ok := infosMap[jwtCreateNode]
	require.True(t, ok)
	require.Equal(t, createInstanceID, info.InstanceID)

	// Now simulate bouncing the apiServer jwtCreateNode - this will cause instanceID to change
	apiServer := servers[0].GetApiServer()
	apiServer.InvalidateInstanceID()

	// Now create another JWT on jwtCreateNode-  this will have the new instanceID in it
	tok = performBasicAuthAndGetJwtOnNode(t, servers, cl, jwtCreateNode)

	// Try and auth it on server 1 - it should be OK as the public key entry should be invalidated
	resp, statusMessage, _ = sendApiRequest(t, "Bearer", tok, servers[jwtAuthNode].GetConfig().HttpApiAddresses[jwtAuthNode], cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)

	// Entry in map should have been invalidated and updated
	newCreateInstanceID := servers[jwtCreateNode].GetApiServer().GetInstanceID()
	infosMap = servers[jwtAuthNode].GetApiServer().GetCachedPubKeyInfos()
	info, ok = infosMap[jwtCreateNode]
	require.True(t, ok)
	require.Equal(t, newCreateInstanceID, info.InstanceID)
}

func testAuthWithMalformedJwt(t *testing.T, servers []*server.Server, cl *http.Client) {
	tok := "giraffes"
	resp, statusMessage, _ := sendApiRequest(t, "Bearer", tok, servers[0].GetConfig().HttpApiAddresses[0], cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "invalid JWT\n", statusMessage)
}

func testAuthWithWellFormedButInvalidJwt(t *testing.T, servers []*server.Server, cl *http.Client) {
	resp, statusMessage, _ := sendApiRequest(t, "Bearer", invalidJwt, servers[0].GetConfig().HttpApiAddresses[0], cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "invalid JWT\n", statusMessage)
}

func testInvalidBasicAuthHeader(t *testing.T, servers []*server.Server, cl *http.Client) {
	resp, statusMessage, _ := sendApiRequest(t, "Basic", "sausages", servers[0].GetConfig().HttpApiAddresses[0], cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "invalid authorization header\n", statusMessage)
}

func testInvalidAuthType(t *testing.T, servers []*server.Server, cl *http.Client) {
	resp, statusMessage, _ := sendApiRequest(t, "Foobars", "sausages", servers[0].GetConfig().HttpApiAddresses[0], cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "invalid authorization method\n", statusMessage)
}

func testQueryEndpointAuthed(t *testing.T, servers []*server.Server, cl *http.Client) {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)

	// create the topic
	_, _, topicName := sendApiRequest(t, "Basic", createBasicAuthHeader(username, password), servers[0].GetConfig().HttpApiAddresses[0], cl)

	reqBody := fmt.Sprintf("(scan all from %s)", topicName)
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, reqBody, "query", "Basic",
		createBasicAuthHeader(username, password), serverAddress, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)
}

func testQueryEndpointNotAuthed(t *testing.T, servers []*server.Server, cl *http.Client) {
	reqBody := `(get "foo" from my-topic)`
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, reqBody, "query", "Basic",
		createBasicAuthHeader("foo", "bar"), serverAddress, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

func testExecEndpointAuthed(t *testing.T, servers []*server.Server, cl *http.Client) {
	username, password, queryName := setupPreparedQuery(t, servers, cl)
	// Exec query
	invocation := &api.PreparedStatementInvocation{
		QueryName: queryName,
	}
	buff, err := json.Marshal(&invocation)
	require.NoError(t, err)
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, string(buff), "exec", "Basic",
		createBasicAuthHeader(username, password), serverAddress, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)
}

func testExecEndpointNotAuthed(t *testing.T, servers []*server.Server, cl *http.Client) {
	_, _, queryName := setupPreparedQuery(t, servers, cl)
	// Exec query
	invocation := &api.PreparedStatementInvocation{
		QueryName: queryName,
	}
	buff, err := json.Marshal(&invocation)
	require.NoError(t, err)
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, string(buff), "exec", "Basic",
		createBasicAuthHeader("foo", "bar"), serverAddress, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

func setupPreparedQuery(t *testing.T, servers []*server.Server, cl *http.Client) (string, string, string) {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)

	// create the topic
	_, _, topicName := sendApiRequest(t, "Basic", createBasicAuthHeader(username, password), servers[0].GetConfig().HttpApiAddresses[0], cl)

	// Prepare a query
	queryName := fmt.Sprintf("my-query-%s", uuid.New().String())
	reqBody := fmt.Sprintf("prepare %s := (scan all from %s)", queryName, topicName)
	sendRequest(t, reqBody, "statement", "Basic", createBasicAuthHeader(username, password), servers[0].GetConfig().HttpApiAddresses[0], cl)
	return username, password, queryName
}

func testWasmRegisterAuthed(t *testing.T, servers []*server.Server, cl *http.Client) {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, "foo", "wasm-register", "Basic",
		createBasicAuthHeader(username, password), serverAddress, cl)
	// Something other than Unauthorized
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "TEK1004 - failed to parse JSON: invalid character 'o' in literal false (expecting 'a')\n", statusMessage)
}

func testWasmRegisterNotAuthed(t *testing.T, servers []*server.Server, cl *http.Client) {
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, "foo", "wasm-register", "Basic",
		createBasicAuthHeader("foo", "bar"), serverAddress, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

func testScramAuthDoesNotNeedAuth(t *testing.T, servers []*server.Server, cl *http.Client) {
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, "foo", "scram-auth", "Basic",
		createBasicAuthHeader("foo", "bar"), serverAddress, cl)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "header X-Tektite-Scram-Auth-Type missing in request\n", statusMessage)
}

func testPubKeyDoesNotNeedAuth(t *testing.T, servers []*server.Server, cl *http.Client) {
	serverAddress := randomServerAddress(servers)
	uri := fmt.Sprintf("https://%s/tektite/pub-key", serverAddress)
	req, err := http.NewRequest(http.MethodGet, uri, nil)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := cl.Do(req)
	require.NoError(t, err)
	defer closeRespBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
}

func testPutUserAuthed(t *testing.T, servers []*server.Server, cl *http.Client) {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, "foo", "put-user", "Basic",
		createBasicAuthHeader(username, password), serverAddress, cl)
	// Something other than Unauthorized
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "malformed JSON\n", statusMessage)
}

func testPutUserNotAuthed(t *testing.T, servers []*server.Server, cl *http.Client) {
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, "foo", "put-user", "Basic",
		createBasicAuthHeader("foo", "bar"), serverAddress, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

func testQueryEndpointAuthNotNeeded(t *testing.T, servers []*server.Server, cl *http.Client) {
	// create the topic
	_, _, topicName := sendApiRequest(t, "", "", servers[0].GetConfig().HttpApiAddresses[0], cl)

	reqBody := fmt.Sprintf("(scan all from %s)", topicName)
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, reqBody, "query", "", "", serverAddress, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)
}

func testExecEndpointAuthNotNeeded(t *testing.T, servers []*server.Server, cl *http.Client) {
	_, _, queryName := setupPreparedQuery(t, servers, cl)
	// Exec query
	invocation := &api.PreparedStatementInvocation{
		QueryName: queryName,
	}
	buff, err := json.Marshal(&invocation)
	require.NoError(t, err)
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, string(buff), "exec", "",
		"", serverAddress, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)
}

func testWasmRegisterAuthNotNeeded(t *testing.T, servers []*server.Server, cl *http.Client) {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, "foo", "wasm-register", "", "", serverAddress, cl)
	// Something other than Unauthorized
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "TEK1004 - failed to parse JSON: invalid character 'o' in literal false (expecting 'a')\n", statusMessage)
}

func testPutUserAuthNotNeeded(t *testing.T, servers []*server.Server, cl *http.Client) {
	serverAddress := randomServerAddress(servers)
	resp, statusMessage := sendRequest(t, "foo", "put-user", "", "", serverAddress, cl)
	// Something other than Unauthorized
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	require.Equal(t, "malformed JSON\n", statusMessage)
}

func testPutUser(t *testing.T, servers []*server.Server, cl *http.Client) {
	doPutUser(t, servers, cl)
}

func doPutUser(t *testing.T, servers []*server.Server, cl *http.Client) (string, string, string, string) {
	adminUser := genUsername()
	adminPassword := genPassword()

	putUserCred(t, servers[0].GetScramManager(), adminUser, adminPassword, auth.AuthenticationSaslScramSha256)
	serverAddress := randomServerAddress(servers)

	// Now we'll put a new user
	username := genUsername()
	password := genPassword()
	storedKey, serverKey, salt := auth.CreateUserScramCreds(password, auth.AuthenticationSaslScramSha256)
	putUser := api.PutUserRequest{
		UserName:   username,
		StoredKey:  base64.StdEncoding.EncodeToString(storedKey),
		ServerKey:  base64.StdEncoding.EncodeToString(serverKey),
		Salt:       salt,
		Iterations: auth.NumIters,
	}
	buff, err := json.Marshal(&putUser)
	require.NoError(t, err)
	resp, statusMessage := sendRequest(t, string(buff), "put-user", "Basic",
		createBasicAuthHeader(adminUser, adminPassword), serverAddress, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)

	// Now we'll hit API using new user creds
	loginServerAddress := randomServerAddress(servers)
	resp, statusMessage, _ = sendApiRequest(t, "Basic", createBasicAuthHeader(username, password), loginServerAddress, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)

	// should be JWT in response
	tok := resp.Header.Get(api.JwtHeaderName)
	require.NotEmpty(t, tok)
	return tok, username, password, loginServerAddress
}

func testUpdateUserPasswordInvalidateJwt(t *testing.T, servers []*server.Server, cl *http.Client) {
	// First we authenticate and get a JWT
	tok, username, _, _ := doPutUser(t, servers, cl)

	// Now update the password
	newPassword := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, newPassword, auth.AuthenticationSaslScramSha256)

	// This should increment the sequence number and make the jwt invalid
	// We also need to invalidate the auth cache
	srv := rand.Intn(len(servers))
	servers[srv].GetApiServer().InvalidateAuthCache()
	address := servers[srv].GetApiServer().ListenAddress()
	resp, statusMessage, _ := sendApiRequest(t, "Bearer", tok, address, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

func testDeleteUserInvalidateJwt(t *testing.T, servers []*server.Server, cl *http.Client) {
	// First we authenticate and get a JWT
	tok, username, _, _ := doPutUser(t, servers, cl)

	// Now delete the user
	err := servers[0].GetScramManager().DeleteUserCredentials(username)
	require.NoError(t, err)

	// This should make the jwt invalid as user won't be found in store
	// We also need to invalidate the auth cache
	srv := rand.Intn(len(servers))
	servers[srv].GetApiServer().InvalidateAuthCache()
	address := servers[srv].GetApiServer().ListenAddress()
	resp, statusMessage, _ := sendApiRequest(t, "Bearer", tok, address, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

func testUpdateUserPasswordAuthRemainsCached(t *testing.T, servers []*server.Server, cl *http.Client) {
	// First we authenticate and get a JWT

	tok, username, _, serverAddress := doPutUser(t, servers, cl)

	// Now update the password
	newPassword := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, newPassword, auth.AuthenticationSaslScramSha256)

	// This should increment the sequence number and make the jwt invalid
	// However the auth will still be in the authentication cache on the server that created to the token
	// So on the other servers, should get unauthorized
	for i := 0; i < len(servers); i++ {
		address := servers[i].GetConfig().HttpApiAddresses[i]
		if address != serverAddress {
			// Not cached so not authenticated
			resp, statusMessage, _ := sendApiRequest(t, "Bearer", tok, address, cl)
			require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
			require.Equal(t, "authentication failed\n", statusMessage)
		} else {
			// Cached so remains authenticated
			resp, statusMessage, _ := sendApiRequest(t, "Bearer", tok, address, cl)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.Equal(t, "", statusMessage)
			// Now invalidate cache and should be not authenticated
			servers[i].GetApiServer().InvalidateAuthCache()
			resp, statusMessage, _ = sendApiRequest(t, "Bearer", tok, address, cl)
			require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
			require.Equal(t, "authentication failed\n", statusMessage)
		}
	}
}

func testDeleteUser(t *testing.T, servers []*server.Server, cl *http.Client) {
	tok, username, password, address := doPutUser(t, servers, cl)

	resp, statusMessage := sendRequest(t, username, "delete-user", "Bearer", tok, address, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)

	// Now try and use username again, using Basic auth
	resp, statusMessage, _ = sendApiRequest(t, "Basic", createBasicAuthHeader(username, password), address, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)

	// And using JWT
	// Need to invalidate auth cache
	for _, s := range servers {
		s.GetApiServer().InvalidateAuthCache()
	}
	resp, statusMessage, _ = sendApiRequest(t, "Bearer", tok, address, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

func testScramAuthWithClient(t *testing.T, servers []*server.Server, _ *http.Client) {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)
	apiClient, err := client.NewClientWithAuth(servers[1].GetApiServer().ListenAddress(), client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}, auth.AuthenticationSaslScramSha256, username, password)
	require.NoError(t, err)
	topicName := fmt.Sprintf("topic-%s", uuid.New().String())
	stmt := fmt.Sprintf("%s := (topic partitions = 10)", topicName)
	err = apiClient.ExecuteStatement(stmt)
	require.NoError(t, err)
}

func testScramAuthUnknownConversationID(t *testing.T, servers []*server.Server, cl *http.Client) {
	testScramAuthError(t, servers, cl, auth.AuthenticationSaslScramSha256, "unknown",
		http.StatusUnauthorized, "authentication failed - no such conversation id\n")
}

func testScramAuthInvalidAuthType(t *testing.T, servers []*server.Server, cl *http.Client) {
	testScramAuthError(t, servers, cl, "unknown", "", http.StatusBadRequest,
		"SCRAM auth type: unknown not supported. Please use SCRAM-SHA-256\n")
}

func testScramAuthError(t *testing.T, servers []*server.Server, cl *http.Client, authType string, conversationID string,
	expectedError int, expectedMessage string) {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[0].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)
	serverAddress := randomServerAddress(servers)
	uri := fmt.Sprintf("https://%s/tektite/%s", serverAddress, "scram-auth")
	req, err := http.NewRequest(http.MethodPost, uri, bytes.NewBufferString("foo"))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set(api.ScramAuthTypeHeaderName, authType)
	req.Header.Set(api.ScramConversationIdHeaderName, conversationID)
	resp, err := cl.Do(req)
	require.NoError(t, err)
	defer closeRespBody(t, resp)
	statusMessage, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	require.Equal(t, expectedError, resp.StatusCode)
	require.Equal(t, expectedMessage, string(statusMessage))
}

func testPutUserWithClient(t *testing.T, servers []*server.Server, _ *http.Client) {
	username, password := doPutUserWithClient(t, servers)

	// Now create another client with auth and the creds of the new user
	userClient, err := client.NewClientWithAuth(servers[1].GetApiServer().ListenAddress(), client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}, auth.AuthenticationSaslScramSha256, username, password)
	require.NoError(t, err)

	topicName := fmt.Sprintf("topic-%s", uuid.New().String())
	stmt := fmt.Sprintf("%s := (topic partitions = 10)", topicName)
	err = userClient.ExecuteStatement(stmt)
	require.NoError(t, err)
}

func doPutUserWithClient(t *testing.T, servers []*server.Server) (string, string) {
	adminUsername := genUsername()
	adminPassword := genPassword()
	putUserCred(t, servers[0].GetScramManager(), adminUsername, adminPassword, auth.AuthenticationSaslScramSha256)
	apiClient, err := client.NewClientWithAuth(servers[1].GetApiServer().ListenAddress(), client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}, auth.AuthenticationSaslScramSha256, adminUsername, adminPassword)
	require.NoError(t, err)

	username := genUsername()
	password := genPassword()
	err = apiClient.PutUser(username, password)
	require.NoError(t, err)

	return username, password
}

func testDeleteUserWithClient(t *testing.T, servers []*server.Server, _ *http.Client) {
	username, password := doPutUserWithClient(t, servers)

	// Now create another client with auth and the creds of the new user
	userClient, err := client.NewClientWithAuth(servers[1].GetApiServer().ListenAddress(), client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}, auth.AuthenticationSaslScramSha256, username, password)
	require.NoError(t, err)

	topicName := fmt.Sprintf("topic-%s", uuid.New().String())
	stmt := fmt.Sprintf("%s := (topic partitions = 10)", topicName)
	err = userClient.ExecuteStatement(stmt)
	require.NoError(t, err)

	// Now delete user
	err = userClient.DeleteUser(username)
	require.NoError(t, err)

	// Now try and create new client with same creds
	_, err = client.NewClientWithAuth(servers[1].GetApiServer().ListenAddress(), client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}, auth.AuthenticationSaslScramSha256, username, password)
	require.Error(t, err)
	require.Equal(t, "SCRAM authentication request returned HTTP error response 401: 401 Unauthorized", err.Error())
}

func TestAuthCacheTimeout(t *testing.T) {
	authCacheTimeout := 750 * time.Millisecond

	objStore := dev.NewInMemStore(0)
	servers, _ := startClusterFast(t, 3, objStore, func(cfg *conf.Config) {
		cfg.AuthenticationEnabled = true
		cfg.AuthenticationCacheTimeout = authCacheTimeout
	})
	cl := createHttpClient(t)
	defer cl.CloseIdleConnections()

	tok, username, _, address := doPutUser(t, servers, cl)

	resp, statusMessage := sendRequest(t, username, "delete-user", "Bearer", tok, address, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)

	// even though user has been deleted, JWT will be valid until auth cache timeout
	resp, statusMessage, _ = sendApiRequest(t, "Bearer", tok, address, cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)

	// Wait for expiry
	time.Sleep(authCacheTimeout)

	// Now should fail auth
	resp, statusMessage, _ = sendApiRequest(t, "Bearer", tok, address, cl)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	require.Equal(t, "authentication failed\n", statusMessage)
}

var invalidJwt string

func init() {
	privateKey, err := rsa.GenerateKey(crand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	claims := jwt.MapClaims{
		"principal": "some-user",
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["nodeID"] = 0
	token.Header["instanceID"] = uuid.New().String()
	tok, err := token.SignedString(privateKey)
	if err != nil {
		panic(err)
	}
	invalidJwt = tok
}

func performBasicAuthAndGetJwt(t *testing.T, servers []*server.Server, cl *http.Client) (string, int) {
	basicAuthServer := rand.Intn(len(servers))
	tok := performBasicAuthAndGetJwtOnNode(t, servers, cl, basicAuthServer)
	return tok, basicAuthServer
}

func performBasicAuthAndGetJwtOnNode(t *testing.T, servers []*server.Server, cl *http.Client, createJWTNode int) string {
	username := genUsername()
	password := genPassword()
	putUserCred(t, servers[rand.Intn(len(servers))].GetScramManager(), username, password, auth.AuthenticationSaslScramSha256)
	resp, statusMessage, _ := sendApiRequest(t, "Basic", createBasicAuthHeader(username, password), servers[createJWTNode].GetConfig().HttpApiAddresses[createJWTNode], cl)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "", statusMessage)
	tok := resp.Header.Get(api.JwtHeaderName)
	require.NotEmpty(t, tok)
	return tok
}

func genUsername() string {
	return fmt.Sprintf("user-%s", uuid.New().String())
}

func genPassword() string {
	return fmt.Sprintf("pwd-%s", uuid.New().String())
}

func randomServerAddress(servers []*server.Server) string {
	nodeID := rand.Intn(len(servers))
	return servers[nodeID].GetConfig().HttpApiAddresses[nodeID]
}

func createBasicAuthHeader(username string, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}

func createHttpClient(t *testing.T) *http.Client {
	t.Helper()
	// Create config for the test client
	caCert, err := os.ReadFile(serverCertPath)
	require.NoError(t, err)
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: true, //nolint:gosec
	}
	cl := &http.Client{}
	cl.Transport = &http2.Transport{
		TLSClientConfig: tlsConfig,
	}
	return cl
}

func closeRespBody(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp != nil {
		err := resp.Body.Close()
		require.NoError(t, err)
	}
}

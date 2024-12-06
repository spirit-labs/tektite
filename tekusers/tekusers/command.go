package tekusers

import (
	"fmt"
	"github.com/spirit-labs/tektite/apiclient"
	auth "github.com/spirit-labs/tektite/auth2"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/kafkaprotocol"
	"strings"
)

var Included = []string{
	"PutUserCredentialsRequest",
	"PutUserCredentialsResponse",
	"DeleteUserRequest",
	"DeleteUserResponse",
}

type UserCommand struct {
	Put    PutUserCommand    `cmd:"" help:"create/update a user"`
	Delete DeleteUserCommand `cmd:"" help:"delete a user"`
}

type PutUserCommand struct {
	Addresses string             `help:"comma separated list of agents to connect to" default:"localhost:9092" required:""`
	TlsConf   conf.ClientTlsConf `help:"tls configuration for connecting to agents" embed:"" prefix:"tls-"`
	Username  string             `help:"unique username of user" required:""`
	Password  string             `help:"password of user" required:""`
}

func tryConnect(servers string) (*apiclient.KafkaApiConnection, error) {
	cl, err := apiclient.NewKafkaApiClient()
	if err != nil {
		return nil, err
	}
	serversSlice := strings.Split(servers, ",")
	for _, server := range serversSlice {
		server := strings.TrimSpace(server)
		conn, err := cl.NewConnection(server)
		if err != nil {
			fmt.Printf("failed to connect to agent: %s - %v\n", server, err)
			continue
		}
		fmt.Printf("connected to agent: %s\n", server)
		return conn, nil
	}
	return nil, fmt.Errorf("failed to connect to any agent")
}

func (u *PutUserCommand) Run() error {
	conn, err := tryConnect(u.Addresses)
	if err != nil {
		return err
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			// Ignore
		}
	}()
	storedKey, serverKey, salt := auth.CreateUserScramCreds(u.Password, auth.AuthenticationSaslScramSha512)
	req := &kafkaprotocol.PutUserCredentialsRequest{
		Username:  common.StrPtr(u.Username),
		StoredKey: storedKey,
		ServerKey: serverKey,
		Salt:      common.StrPtr(salt),
		Iters:     auth.NumIters,
	}
	resp := &kafkaprotocol.PutUserCredentialsResponse{}
	r, err := conn.SendRequest(req, kafkaprotocol.ApiKeyPutUserCredentialsRequest, 0, resp)
	if err != nil {
		return err
	}
	resp = r.(*kafkaprotocol.PutUserCredentialsResponse)
	if resp.ErrorCode == kafkaprotocol.ErrorCodeNone {
		fmt.Println("OK")
	} else {
		return fmt.Errorf("failed to create user: %s", common.SafeDerefStringPtr(resp.ErrorMessage))
	}
	return nil
}

type DeleteUserCommand struct {
	Addresses string             `help:"comma separated list of agents to connect to" default:"localhost:9092" required:""`
	TlsConf   conf.ClientTlsConf `help:"tls configuration for connecting to agents" embed:"" prefix:"tls-"`
	Username  string             `help:"unique username of user" required:""`
}

func (u *DeleteUserCommand) Run() error {
	conn, err := tryConnect(u.Addresses)
	if err != nil {
		return err
	}
	defer func() {
		err := conn.Close()
		if err != nil {
			// Ignore
		}
	}()
	req := &kafkaprotocol.DeleteUserRequest{
		Username: common.StrPtr(u.Username),
	}
	resp := &kafkaprotocol.DeleteUserResponse{}
	r, err := conn.SendRequest(req, kafkaprotocol.ApiKeyDeleteUserRequest, 0, resp)
	if err != nil {
		return err
	}
	resp = r.(*kafkaprotocol.DeleteUserResponse)
	if resp.ErrorCode == kafkaprotocol.ErrorCodeNone {
		fmt.Println("OK")
	} else {
		fmt.Println(fmt.Sprintf("failed to delete user - %s", common.SafeDerefStringPtr(resp.ErrorMessage)))
	}
	return nil
}

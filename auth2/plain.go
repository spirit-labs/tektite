package auth

import (
	"bytes"
	"github.com/pkg/errors"
	log "github.com/spirit-labs/tektite/logger"
)

type PlainSaslConversation struct {
	scramManager *ScramManager
	principal    string
}

func (p *PlainSaslConversation) Process(request []byte) (resp []byte, complete bool, failed bool) {
	_, username, password, err := decodeSaslPlain(request)
	if err != nil {
		log.Warnf("invalid SASL/PLAIN request: %v", err)
		return nil, false, true
	}
	_, valid, err := p.scramManager.AuthenticateWithUserPwd(username, password)
	if err != nil {
		log.Warnf("failed to authenticate using SASL/PLAIN: %v", err)
		return nil, false, true
	}
	if valid {
		p.principal = username
		return nil, true, false
	} else {
		return nil, false, true
	}
}

func (p *PlainSaslConversation) Principal() string {
	return p.principal
}

func decodeSaslPlain(buffer []byte) (authzid, username, password string, err error) {
	parts := bytes.Split(buffer, []byte{0})
	if len(parts) != 3 {
		return "", "", "", errors.New("invalid SASL/PLAIN message format")
	}
	authzid = string(parts[0])
	username = string(parts[1])
	password = string(parts[2])
	if username == "" || password == "" {
		return "", "", "", errors.New("username and password must not be empty")
	}
	return authzid, username, password, nil
}

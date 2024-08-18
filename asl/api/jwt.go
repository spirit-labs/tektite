package api

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"github.com/pkg/errors"
	"io"
)

func (s *HTTPAPIServer) createJwt(principal string, credsSequence int) (string, error) {
	claims := jwt.MapClaims{
		"principal": principal,
		"sequence":  credsSequence,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["nodeID"] = s.nodeID
	token.Header["instanceID"] = s.jwtInstanceID
	return token.SignedString(s.jwtPrivateKey)
}

func (s *HTTPAPIServer) verifyJwt(tokenString string) (jwt.MapClaims, bool, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		nid, ok := token.Header["nodeID"]
		if !ok {
			return nil, errors.New("invalid JWT - does not contain nodeID header")
		}
		nf, ok := nid.(float64) // JSON decodes number as float64
		if !ok {
			return nil, errors.New("invalid JWT - nodeID header does not contain a number")
		}
		iid, ok := token.Header["instanceID"]
		if !ok {
			return nil, errors.New("invalid JWT - does not contain instanceID header")
		}
		instanceID, ok := iid.(string)
		if !ok {
			return nil, errors.New("invalid JWT - instanceID header does not contain a string")
		}
		nodeID := int(nf)
		if nodeID == s.nodeID {
			// Created on this node
			return &s.jwtPrivateKey.PublicKey, nil
		}
		pubKeyInfo, err := s.publicKey(nodeID, instanceID)
		if err != nil {
			return nil, err
		}
		return pubKeyInfo.PubKey, nil
	})
	if err != nil {
		return nil, false, err
	}
	if token.Valid {
		return token.Claims.(jwt.MapClaims), true, nil
	}
	return nil, false, nil
}

func (s *HTTPAPIServer) publicKeyRLock(nodeID int, instanceID string) (JwtPublicKeyInfo, bool) {
	s.jwtPubKeysLock.RLock()
	defer s.jwtPubKeysLock.RUnlock()
	pubKeyInfo, ok := s.jwtPublicKeys[nodeID]
	if ok && pubKeyInfo.InstanceID == instanceID {
		// We have the public key and it has the right instanceID
		// The instanceID changes every time the server is restarted, so if we have cached an old public key
		// for a node, and we receive a JWT signed by the node after it was restarted then we won't have the right
		// public key. The instanceID allows us to check this.
		return pubKeyInfo, true
	}
	return pubKeyInfo, false
}

func (s *HTTPAPIServer) publicKeyLock(nodeID int, instanceID string) (JwtPublicKeyInfo, error) {
	s.jwtPubKeysLock.Lock()
	defer s.jwtPubKeysLock.Unlock()
	pubKeyInfo, ok := s.jwtPublicKeys[nodeID]
	if ok {
		if pubKeyInfo.InstanceID == instanceID {
			return pubKeyInfo, nil
		}
		// We have an old public key cached - invalidate it
		delete(s.jwtPublicKeys, nodeID)
	}
	// We need to get the public key from the remote node
	pubKeyInfo, err := s.getRemotePublicKey(nodeID)
	if err != nil {
		return JwtPublicKeyInfo{}, err
	}
	s.jwtPublicKeys[nodeID] = pubKeyInfo
	return pubKeyInfo, nil
}

func (s *HTTPAPIServer) publicKey(nodeID int, instanceID string) (JwtPublicKeyInfo, error) {
	// Happy path with RLock
	pubKeyInfo, ok := s.publicKeyRLock(nodeID, instanceID)
	if ok {
		return pubKeyInfo, nil
	}
	// Sad path with Lock
	return s.publicKeyLock(nodeID, instanceID)
}

func (s *HTTPAPIServer) getRemotePublicKey(nodeID int) (JwtPublicKeyInfo, error) {
	url := fmt.Sprintf("https://%s/tektite/pub-key", s.listenAddresses[nodeID])
	resp, err := s.httpClient.Get(url)
	if err != nil {
		return JwtPublicKeyInfo{}, err
	}
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return JwtPublicKeyInfo{}, err
	}
	var pkResp pubKeyResponse
	if err := json.Unmarshal(bytes, &pkResp); err != nil {
		return JwtPublicKeyInfo{}, err
	}
	publicKeyBytes, err := base64.StdEncoding.DecodeString(pkResp.PublicKey)
	if err != nil {
		return JwtPublicKeyInfo{}, err
	}
	pubKey, err := x509.ParsePKCS1PublicKey(publicKeyBytes)
	if err != nil {
		return JwtPublicKeyInfo{}, err
	}
	return JwtPublicKeyInfo{PubKey: pubKey, InstanceID: pkResp.InstanceID}, nil
}

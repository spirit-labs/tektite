package api

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"github.com/golang-jwt/jwt/v4"
	"github.com/google/uuid"
	"testing"
)

func BenchmarkVerifyJwt(b *testing.B) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	claims := jwt.MapClaims{
		"principal": "george",
		//"exp":      time.Now().Add(time.Hour * 1).Unix(), // Token expires in 1 hour
	}
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	token.Header["nodeID"] = 12345
	token.Header["instanceID"] = uuid.New().String()
	tok, err := token.SignedString(privateKey)
	if err != nil {
		panic(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token, err := jwt.Parse(tok, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}
			return &privateKey.PublicKey, nil
		})
		if err != nil {
			panic(err)
		}
		if !token.Valid {
			panic("not valid")
		}
	}
}

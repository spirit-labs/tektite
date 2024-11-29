package auth

import (
	"github.com/stretchr/testify/require"
	"github.com/xdg-go/scram"
	"testing"
)

// TestScram - this just runs a client/server conv using xdg-go as a sanity check, it does not test our end-end scram
// that is done in the integration tests
func TestScram(t *testing.T) {
	username := "some_user"
	password := "some_password"
	cl, err := scram.SHA256.NewClient(username, password, "")
	require.NoError(t, err)

	salt := "salty"
	storedCreds := cl.GetStoredCredentials(scram.KeyFactors{
		Salt:  salt,
		Iters: 4096,
	})

	s, err := scram.SHA256.NewServer(func(s string) (scram.StoredCredentials, error) {
		return storedCreds, nil
	})
	require.NoError(t, err)

	clConv := cl.NewConversation()
	sConv := s.NewConversation()

	r, err := clConv.Step("")
	require.NoError(t, err)

	r, err = sConv.Step(r)
	require.NoError(t, err)

	r, err = clConv.Step(r)
	require.NoError(t, err)

	r, err = sConv.Step(r)
	require.NoError(t, err)

	r, err = clConv.Step(r)
	require.NoError(t, err)

	require.True(t, clConv.Done())
	require.True(t, clConv.Valid())
}

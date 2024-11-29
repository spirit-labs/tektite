package auth

type Context struct {
	Principal     *string
	Authenticated bool
}

const (
	AuthenticationTLS             = "tls"
	AuthenticationSaslScramSha256 = "SCRAM-SHA-256"
	AuthenticationSaslScramSha512 = "SCRAM-SHA-512"
)

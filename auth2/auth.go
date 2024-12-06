package auth

type Context struct {
	Principal     string
	Authenticated bool
}

const (
	AuthenticationSaslScramSha256 = "SCRAM-SHA-256"
	AuthenticationSaslScramSha512 = "SCRAM-SHA-512"
	AuthenticationSaslPlain       = "PLAIN"
)

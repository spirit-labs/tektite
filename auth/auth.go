package auth

type Context struct {
	Principal  *string
	Authorised bool
}

const (
	AuthenticationTLS = "tls"
)

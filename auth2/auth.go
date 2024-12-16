package auth

import "github.com/spirit-labs/tektite/acls"

type Context struct {
	Principal     string
	Authenticated bool
	authCache     *UserAuthCache
	RequiresAuth bool
}

func (c *Context) SetAuthenticated(principal string, authCache *UserAuthCache) {
	c.Principal = principal
	c.Authenticated = true
	c.authCache = authCache
}

func (c *Context) Authorize(resourceType acls.ResourceType, resourceName string, operation acls.Operation) (bool, error) {
	if !c.RequiresAuth {
		return true, nil
	}
	if !c.Authenticated {
		return false, nil
	}
	return c.authCache.Authorize(resourceType, resourceName, operation)
}

const (
	AuthenticationSaslScramSha256 = "SCRAM-SHA-256"
	AuthenticationSaslScramSha512 = "SCRAM-SHA-512"
	AuthenticationSaslPlain       = "PLAIN"
)

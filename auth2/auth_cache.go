package auth

import (
	"github.com/spirit-labs/tektite/acls"
	"github.com/spirit-labs/tektite/asl/arista"
	"sync"
	"time"
)

type ControlClient interface {
	Authorise(principal string, resourceType acls.ResourceType, resourceName string, operation acls.Operation) (bool, error)
}

type ControlClientFactory func() (ControlClient, error)

type UserAuthCache struct {
	lock                 sync.RWMutex
	principal            string
	authTimeout          time.Duration
	authorisations       map[acls.ResourceType]map[string][]ResourceAuthorization
	controlClientFactory ControlClientFactory
}

func NewUserAuthCache(principal string, controlClientFactory ControlClientFactory, authTimeout time.Duration) *UserAuthCache {
	return &UserAuthCache{
		principal:            principal,
		authorisations:       make(map[acls.ResourceType]map[string][]ResourceAuthorization),
		controlClientFactory: controlClientFactory,
		authTimeout:          authTimeout,
	}
}

type ResourceAuthorization struct {
	operation  acls.Operation
	authorised bool
	authTime   uint64
}

func (r *ResourceAuthorization) isExpired(now uint64, timeout time.Duration) bool {
	return now-r.authTime >= uint64(timeout.Nanoseconds())
}

func (u *UserAuthCache) Authorize(resourceType acls.ResourceType, resourceName string, operation acls.Operation) (bool, error) {
	now := arista.NanoTime()
	authorised, cached := u.authoriseFromCache(resourceType, resourceName, operation, now)
	if cached {
		return authorised, nil
	}
	u.lock.Lock()
	defer u.lock.Unlock()
	// authorise from cache again to avoid race between dropping rlock and getting wlock
	authorised, cached = u.authoriseFromCache0(resourceType, resourceName, operation, now)
	if cached {
		return authorised, nil
	}
	conn, err := u.controlClientFactory()
	if err != nil {
		return false, err
	}
	authorised, err = conn.Authorise(u.principal, resourceType, resourceName, operation)
	if err != nil {
		return false, err
	}
	resourceTypeMap, ok := u.authorisations[resourceType]
	if !ok {
		resourceTypeMap = map[string][]ResourceAuthorization{}
		u.authorisations[resourceType] = resourceTypeMap
	}
	authorisations := resourceTypeMap[resourceName]
	var authsPruned []ResourceAuthorization
	for _, auth := range authorisations {
		// Remove the auth if same operation as could be an expired one, and we don't want duplicates
		if auth.operation != operation {
			authsPruned = append(authsPruned, auth)
		}
	}
	authsPruned = append(authsPruned, ResourceAuthorization{
		operation:  operation,
		authorised: authorised,
		authTime:   arista.NanoTime(),
	})
	resourceTypeMap[resourceName] = authsPruned
	return authorised, nil
}

func (u *UserAuthCache) authoriseFromCache(resourceType acls.ResourceType, resourceName string, operation acls.Operation, now uint64) (authorised bool, cached bool) {
	u.lock.RLock()
	defer u.lock.RUnlock()
	return u.authoriseFromCache0(resourceType, resourceName, operation, now)
}

func (u *UserAuthCache) authoriseFromCache0(resourceType acls.ResourceType, resourceName string, operation acls.Operation, now uint64) (authorised bool, cached bool) {
	resourceTypeMap, ok := u.authorisations[resourceType]
	if ok {
		authorizations, ok := resourceTypeMap[resourceName]
		if ok {
			for _, auth := range authorizations {
				if auth.operation == operation && !auth.isExpired(now, u.authTimeout) {
					return auth.authorised, true
				}
			}
		}
	}
	return false, false
}

func (u *UserAuthCache) CheckExpired() {
	u.lock.Lock()
	defer u.lock.Unlock()
	now := arista.NanoTime()
	for resourceType, resourceMap := range u.authorisations {
		for resourceName, auths := range resourceMap {
			hasExpired := false
			for _, auth := range auths {
				if auth.isExpired(now, u.authTimeout) {
					hasExpired = true
					break
				}
			}
			if hasExpired {
				var newAuths []ResourceAuthorization
				for _, auth := range auths {
					if !auth.isExpired(now, u.authTimeout) {
						newAuths = append(newAuths, auth)
					}
				}
				if len(newAuths) > 0 {
					resourceMap[resourceName] = newAuths
				} else {
					delete(resourceMap, resourceName)
					if len(resourceMap) == 0 {
						delete(u.authorisations, resourceType)
					}
				}
			}
		}
	}
}


type UserAuthCaches struct {
	lock sync.RWMutex
	started bool
	authCaches map[string]*UserAuthCache
	authTimeout time.Duration
	controlClientFactory ControlClientFactory
	checkTimer *time.Timer
}

func NewUserAuthCaches(authTimeout time.Duration, controlClientFactory ControlClientFactory) *UserAuthCaches {
	return &UserAuthCaches{
		authTimeout:          authTimeout,
		controlClientFactory: controlClientFactory,
		authCaches:           make(map[string]*UserAuthCache),
	}
}

func (a *UserAuthCaches) Start() {
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.started {
		return
	}
	a.scheduleTimer()
	a.started = true
}

func (a *UserAuthCaches) Stop() {
	a.lock.Lock()
	defer a.lock.Unlock()
	if !a.started {
		return
	}
	a.checkTimer.Stop()
	a.started = false
}

func (a *UserAuthCaches) scheduleTimer() {
	a.checkTimer = time.AfterFunc(a.authTimeout, func() {
		a.lock.Lock()
		defer a.lock.Unlock()
		if !a.started {
			return
		}
		a.checkExpired()
		a.scheduleTimer()
	})
}

func (a *UserAuthCaches) checkExpired() {
	for _, cache := range a.authCaches {
		cache.CheckExpired()
	}
}

func (a *UserAuthCaches) GetAuthCache(principal string) *UserAuthCache {
	authCache := a.getAuthCache(principal)
	if authCache != nil {
		return authCache
	}
	return a.createAuthCache(principal)
}

func (a *UserAuthCaches) getAuthCache(principal string) *UserAuthCache {
	a.lock.RLock()
	defer a.lock.RUnlock()
	authCache, ok := a.authCaches[principal]
	if ok {
		return authCache
	}
	return nil
}

func (a *UserAuthCaches) createAuthCache(principal string) *UserAuthCache {
	a.lock.Lock()
	defer a.lock.Unlock()
	authCache, ok := a.authCaches[principal] // check again to avoid creation race
	if ok {
		return authCache
	}
	authCache = NewUserAuthCache(principal, a.controlClientFactory, a.authTimeout)
	a.authCaches[principal] = authCache
	return authCache
}


package auth

type SaslAuthManager struct {
	scramManager *ScramManager
}

func NewSaslAuthManager(scramManager *ScramManager) (*SaslAuthManager, error) {
	return &SaslAuthManager{
		scramManager: scramManager,
	}, nil
}

func (s *SaslAuthManager) CreateConversation(mechanism string) (SaslConversation, bool, error) {
	if mechanism == s.scramManager.authType {
		conv, err := s.scramManager.NewConversation()
		if err != nil {
			return nil, false, err
		}
		return conv, true, nil
	}
	return nil, false, nil
}

func (s *SaslAuthManager) ScramAuthType() string {
	return s.scramManager.AuthType()
}

type SaslConversation interface {
	Process(request []byte) (resp []byte, complete bool, failed bool)
	Principal() string
}

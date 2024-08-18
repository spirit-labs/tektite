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
	switch mechanism {
	case AuthenticationSaslScramSha256:
		conv, err := s.scramManager.NewConversation(ScramAuthTypeSHA256)
		if err != nil {
			return nil, false, err
		}
		return conv, true, nil
	case AuthenticationSaslScramSha512:
		conv, err := s.scramManager.NewConversation(ScramAuthTypeSHA512)
		if err != nil {
			return nil, false, err
		}
		return conv, true, nil
	default:
		return nil, false, nil
	}
}

type SaslConversation interface {
	Process(request []byte) (resp []byte, complete bool, failed bool)
	Principal() string
}

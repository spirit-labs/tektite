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
	case AuthenticationSaslScramSha512:
		conv, err := s.scramManager.NewConversation()
		if err != nil {
			return nil, false, err
		}
		return conv, true, nil
	case AuthenticationSaslPlain:
		conv := &PlainSaslConversation{
			scramManager: s.scramManager,
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

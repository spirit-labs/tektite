package auth

type SaslAuthManager struct {
}

const (
	SASL_SCRAM = "SASL_SCRAM"
)

func (s *SaslAuthManager) CreateConversation(mechanism string) (SaslConversation, bool) {
	switch mechanism {
	case SASL_SCRAM:
		return &saslScramConversion{}, true
	default:
		return nil, false
	}
}

type SaslConversation interface {
	Process(request []byte) (response []byte, complete bool)
	Principal() string
}

type saslScramConversion struct {
}

func (s *saslScramConversion) Process(request []byte) ([]byte, bool) {
	//TODO implement me
	panic("implement me")
}

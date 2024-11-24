package agent

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestInvalidMembershipUpdateInterval(t *testing.T) {
	conf := CommandConf{}
	conf.MembershipEvictionIntervalMs = 100
	conf.MembershipUpdateIntervalMs = 0
	_, err := CreateConfFromCommandConf(conf)
	require.Error(t, err)
	require.Equal(t, "invalid value for membership-update-interval-ms must be >= 1 ms", err.Error())

	conf.MembershipUpdateIntervalMs = -1
	_, err = CreateConfFromCommandConf(conf)
	require.Error(t, err)
	require.Equal(t, "invalid value for membership-update-interval-ms must be >= 1 ms", err.Error())
}

func TestInvalidMembershipEvictionInterval(t *testing.T) {
	conf := CommandConf{}
	conf.MembershipUpdateIntervalMs = 100
	conf.MembershipEvictionIntervalMs = 0
	_, err := CreateConfFromCommandConf(conf)
	require.Error(t, err)
	require.Equal(t, "invalid value for membership-eviction-interval-ms must be >= 1 ms", err.Error())

	conf.MembershipEvictionIntervalMs = -1
	_, err = CreateConfFromCommandConf(conf)
	require.Error(t, err)
	require.Equal(t, "invalid value for membership-eviction-interval-ms must be >= 1 ms", err.Error())
}

func TestInvalidConsumerGroupInitialJoinDelayInterval(t *testing.T) {
	conf := CommandConf{}
	conf.MembershipUpdateIntervalMs = 100
	conf.MembershipEvictionIntervalMs = 100
	conf.ConsumerGroupInitialJoinDelayMs = -1
	_, err := CreateConfFromCommandConf(conf)
	require.Error(t, err)
	require.Equal(t, "invalid value for consumer-group-initial-join-delay-ms must be >= 0 ms", err.Error())
}

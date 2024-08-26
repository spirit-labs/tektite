//go:build integration

package integration

import (
	"fmt"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/spirit-labs/tektite/kafka/fake"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestServer(t *testing.T) {
	fk := &fake.Kafka{}

	clientTLSConfig := client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	servers, tearDown := startCluster(t, 3, fk)
	defer tearDown(t)
	client, err := client.NewClient(servers[0].GetConfig().HttpApiAddresses[0], clientTLSConfig)
	require.NoError(t, err)
	defer client.Close()

	// We create a stream, scan it, then delete it in a loop
	// This exercises the barrier logic and makes sure barriers still propagate and versions complete as streams are
	// deployed / undeployed, without getting stuck
	for i := 0; i < 10; i++ {
		streamName := fmt.Sprintf("test_stream-%d", i)

		topic, err := fk.CreateTopic("test_topic", 10)
		require.NoError(t, err)

		_, err = client.ExecuteQuery(fmt.Sprintf("(scan all from %s)", streamName))
		require.Error(t, err)
		require.Equal(t, fmt.Sprintf(`unknown table or stream '%s' (line 1 column 16):
(scan all from %s)
               ^`, streamName, streamName), err.Error())

		err = client.ExecuteStatement(fmt.Sprintf(`%s := 
		(bridge from test_topic
			partitions = 10
			props = ()
		) -> (partition by key partitions = 16) -> (store stream)`, streamName))
		require.NoError(t, err)

		qr, err := client.ExecuteQuery(fmt.Sprintf("(scan all from %s)", streamName))
		require.NoError(t, err)
		require.Equal(t, 0, qr.RowCount())

		numMessages := 10
		for i := 0; i < numMessages; i++ {
			// Generate some JSON messages
			var msg kafka.Message
			msg.Key = []byte(fmt.Sprintf("key%d", i))
			msg.Value = []byte(fmt.Sprintf("value%d", i))
			err := topic.Push(&msg)
			require.NoError(t, err)
		}

		ok, err := testutils.WaitUntilWithError(func() (bool, error) {
			qr, err = client.ExecuteQuery(fmt.Sprintf("(scan all from %s)", streamName))
			if err != nil {
				return false, err
			}
			return qr.RowCount() == numMessages, nil
		}, 10*time.Second, 1*time.Second)
		require.True(t, ok)
		require.NoError(t, err)

		err = client.ExecuteStatement(fmt.Sprintf(`delete(%s)`, streamName))
		require.NoError(t, err)

		err = fk.DeleteTopic("test_topic")
		require.NoError(t, err)
	}

}

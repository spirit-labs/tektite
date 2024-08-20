//go:build integration

package integration

import (
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/server"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafka"
	"github.com/spirit-labs/tektite/kafka/fake"
	log "github.com/spirit-labs/tektite/logger"
	"github.com/spirit-labs/tektite/sanity"
	"github.com/spirit-labs/tektite/testutils"
	"github.com/stretchr/testify/require"
	"math/rand"
	"testing"
	"time"
)

func TestFailoverReplicationQueues(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	testFailoverReplicationQueues(t, false)
}

func TestFailoverReplicationQueuesFailLevelManagerNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	testFailoverReplicationQueues(t, true)
}

func testFailoverReplicationQueues(t *testing.T, failLevelManager bool) {

	servers, tearDown := setupServers(t, nil)
	defer tearDown(t)
	failNode, clientNode := getFailAndClientNodes(failLevelManager, servers)
	client := createClient(t, clientNode, servers)
	defer client.Close()

	err := client.ExecuteStatement(`topic1 := (kafka in partitions=100) -> (store stream)`)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	startTime := time.Now().UTC()

	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": servers[0].GetConfig().KafkaServerListenerConfig.Addresses[0],
		"acks":              "all"})
	require.NoError(t, err)
	defer producer.Close()

	numMessages := 1000

	// First we send messages to topic1 - this has one partition, so offsets should come back in contiguous order
	_, err = sendMessagesGoClient(numMessages, "topic1", producer)
	require.NoError(t, err)

	log.Debug("sent all the messages")

	waitForIncrementingRows(t, "topic1", numMessages, client, startTime)

	log.Debug("**** stopping server")

	// Now stop one of the nodes - this should cause a failover
	err = servers[failNode].Stop()
	require.NoError(t, err)

	sleepRandom(2000 * time.Millisecond)

	log.Debug("*** waiting for rows after failover")

	waitForIncrementingRows(t, "topic1", numMessages, client, startTime)
}

func TestFailoverWithKafkaIn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	testFailoverWithKafkaIn(t, false)
}

func TestFailoverWithKafkaInFailLevelManagerNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	testFailoverWithKafkaIn(t, true)
}

func testFailoverWithKafkaIn(t *testing.T, failLevelManager bool) {

	fakeKafka := &fake.Kafka{}

	servers, tearDown := setupServers(t, fakeKafka)
	defer tearDown(t)
	failNode, clientNode := getFailAndClientNodes(failLevelManager, servers)
	client := createClient(t, clientNode, servers)
	defer client.Close()

	topic, err := fakeKafka.CreateTopic("test_topic", 40)
	require.NoError(t, err)

	tsl := `test_stream := 
		(bridge from test_topic
			partitions = 40
			props = ()
		) -> (store stream)`
	err = client.ExecuteStatement(tsl)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	startTime := time.Now().UTC()

	numMessages := 1000

	// Send some messages to fake topic
	for i := 0; i < numMessages; i++ {
		key := []byte(fmt.Sprintf("key%05d", i))
		value := []byte(fmt.Sprintf("value%05d", i))
		msg := &kafka.Message{
			TimeStamp: time.Now(),
			Key:       key,
			Value:     value,
		}
		err = topic.Push(msg)
		require.NoError(t, err)
	}

	log.Debug("sent all the messages")

	waitForIncrementingRows(t, "test_stream", numMessages, client, startTime)

	log.Debug("**** stopping server")

	// Now stop one of the nodes - this should cause a failover
	err = servers[failNode].Stop()
	require.NoError(t, err)

	sleepRandom(2000 * time.Millisecond)

	log.Debug("*** waiting for rows after failover")

	waitForIncrementingRows(t, "test_stream", numMessages, client, startTime)
}

func waitForIncrementingRows(t *testing.T, tableName string, numMessages int, cl client.Client, startTime time.Time) {
	waitForRowsWithVerifier(t, fmt.Sprintf("(scan all from %s) -> (sort by key)", tableName),
		func(qr client.QueryResult) bool {
			for i := 0; i < qr.RowCount(); i++ {
				row := qr.Row(i)
				expectedKey := fmt.Sprintf("key%05d", i)
				expectedValue := fmt.Sprintf("value%05d", i)
				eventTime := row.TimestampVal(1)
				key := string(row.BytesVal(2))
				value := string(row.BytesVal(4))

				//log.Debugf("got key: %s value: %s", key, value)

				if expectedKey != key {
					log.Errorf("expected key: %s got key: %s", expectedKey, key)
					return false
				}
				require.Equal(t, expectedValue, value)
				now := time.Now().UTC()
				require.GreaterOrEqual(t, eventTime.Val, startTime.UnixMilli())
				require.Less(t, eventTime.Val, now.UnixMilli()+1000)
			}
			log.Debugf("waited and got %d rows", qr.RowCount())
			return qr.RowCount() == numMessages
		}, cl)
}

func TestFailoverReplicationQueuesWithAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	testFailoverReplicationQueuesWithAggregation(t, false)
}

func TestFailoverReplicationQueuesWithAggregationFailLevelManagerNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	testFailoverReplicationQueuesWithAggregation(t, true)
}

func testFailoverReplicationQueuesWithAggregation(t *testing.T, failLevelManager bool) {

	servers, tearDown := setupServers(t, nil)
	defer tearDown(t)
	failNode, clientNode := getFailAndClientNodes(failLevelManager, servers)
	client := createClient(t, clientNode, servers)
	defer client.Close()

	sanity.ClearStores()

	defer func() {
		r := recover()
		if r == nil {
			return // no panic underway
		}
		log.Errorf("caught panic in agg failover test %v", r)
		panic(r)
	}()

	tsl := `sensor_agg :=  (kafka in partitions=40)
			-> (project
				json_int("id", val) as id,
				json_string("country", val) as country,
				json_int("temperature", val) as temperature)
			-> (partition by country partitions=20)
			-> (aggregate avg(temperature), max(temperature), min(temperature) by country)`

	err := client.ExecuteStatement(tsl)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": servers[0].GetConfig().KafkaServerListenerConfig.Addresses[0],
		"acks":              "all"})
	require.NoError(t, err)
	defer producer.Close()

	numMessages := 1000

	// First we send messages to topic1 - this has one partition, so offsets should come back in contiguous order
	_, err = sendMessagesForAggregation(numMessages, "sensor_agg", producer)
	require.NoError(t, err)
	log.Debug("sent messagaes")

	waitForAggRows(t, "sensor_agg", 20, client)

	log.Debugf("**** stopping server %d", failNode)

	// Now stop one of the nodes - this should cause a failover
	err = servers[failNode].Stop()
	require.NoError(t, err)

	log.Debug("**** stopped server")

	sleepRandom(2000 * time.Millisecond)

	log.Debug("*** waiting for rows after failover")

	waitForAggRows(t, "sensor_agg", 20, client)
}

func TestFailoverWithKafkaInWithAggregation(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	testFailoverWithKafkaInWithAggregation(t, false)
}

func TestFailoverWithKafkaInWithAggregationFailLevelManagerNode(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}
	testFailoverWithKafkaInWithAggregation(t, true)
}

func testFailoverWithKafkaInWithAggregation(t *testing.T, failLevelManager bool) {

	fakeKafka := &fake.Kafka{}

	servers, tearDown := setupServers(t, fakeKafka)
	defer tearDown(t)
	failNode, clientNode := getFailAndClientNodes(failLevelManager, servers)
	client := createClient(t, clientNode, servers)
	defer client.Close()

	topic, err := fakeKafka.CreateTopic("test_topic", 40)
	require.NoError(t, err)

	tsl := `sensor_agg :=  (bridge from test_topic
				partitions = 40
				props = ()
			)
			-> (project
				json_int("id", val) as id,
				json_string("country", val) as country,
				json_int("temperature", val) as temperature)
			-> (partition by country partitions=20)
			-> (aggregate avg(temperature), max(temperature), min(temperature) by country)`

	err = client.ExecuteStatement(tsl)
	require.NoError(t, err)

	time.Sleep(1 * time.Second)

	numMessages := 1000
	numSensors := 100
	numCountries := 20

	for i := 0; i < numMessages; i++ {
		sensorID := i % numSensors
		countryID := i % numCountries
		key := []byte(fmt.Sprintf("sensor-%05d", sensorID))
		country := fmt.Sprintf("country-%05d", countryID)
		temperature := ((i + 7) % 35) - 10
		value := []byte(fmt.Sprintf(`{"id":%d,"country":"%s","temperature":%d}`, i, country, temperature))
		msg := kafka.Message{
			TimeStamp: time.Now(),
			Key:       key,
			Value:     value,
		}
		err = topic.Push(&msg)
		require.NoError(t, err)
	}

	log.Debug("sent all the messages")

	waitForAggRows(t, "sensor_agg", 20, client)

	log.Debug("**** stopping server")

	// Now stop one of the nodes - this should cause a failover
	err = servers[failNode].Stop()
	require.NoError(t, err)

	sleepRandom(2000 * time.Millisecond)

	log.Debug("*** waiting for rows after failover")

	waitForAggRows(t, "sensor_agg", 20, client)
}

func sleepRandom(maxDur time.Duration) {
	// Add a random delay, so we can hit different parts of the failure process. Also without a delay the test can exit
	// after receiving all the expected rows in the next query, before failure has really occurred.
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	time.Sleep(time.Duration(r.Intn(int(maxDur))))
}

func sendMessagesForAggregation(numMessages int, topicName string, producer *kafkago.Producer) ([]*kafkago.Message, error) {
	deliveryChan := make(chan kafkago.Event, numMessages)

	var msgs []*kafkago.Message
	numSensors := 100
	numCountries := 20
	for i := 0; i < numMessages; i++ {
		sensorID := i % numSensors
		countryID := i % numCountries
		key := []byte(fmt.Sprintf("sensor-%05d", sensorID))
		country := fmt.Sprintf("country-%05d", countryID)
		temperature := ((i + 7) % 35) - 10
		value := []byte(fmt.Sprintf(`{"id":%d,"country":"%s","temperature":%d}`, i, country, temperature))
		err := producer.Produce(&kafkago.Message{
			TopicPartition: kafkago.TopicPartition{Topic: &topicName, Partition: kafkago.PartitionAny},
			Key:            key,
			Value:          value},
			deliveryChan,
		)
		if err != nil {
			return nil, err
		}
		// Note, we send messages one by one and wait for receipt on each one. Yes, this is slow, but it means it creates
		// lots of batches on the server which exercises the replication and failover/replay logic better
		e := <-deliveryChan
		m := e.(*kafkago.Message)
		if m.TopicPartition.Error != nil {
			return nil, m.TopicPartition.Error
		}
		msgs = append(msgs, m)
	}
	return msgs, nil
}

func waitForAggRows(t *testing.T, tableName string, numRows int, cl client.Client) {
	expected := []string{"country:country-00000 avg:6.800000 max:22 min:-8",
		"country:country-00001 avg:7.800000 max:23 min:-7",
		"country:country-00002 avg:8.800000 max:24 min:-6",
		"country:country-00003 avg:4.900000 max:20 min:-10",
		"country:country-00004 avg:5.900000 max:21 min:-9",
		"country:country-00005 avg:6.900000 max:22 min:-8",
		"country:country-00006 avg:7.900000 max:23 min:-7",
		"country:country-00007 avg:8.900000 max:24 min:-6",
		"country:country-00008 avg:5.000000 max:20 min:-10",
		"country:country-00009 avg:6.000000 max:21 min:-9",
		"country:country-00010 avg:7.000000 max:22 min:-8",
		"country:country-00011 avg:8.000000 max:23 min:-7",
		"country:country-00012 avg:9.000000 max:24 min:-6",
		"country:country-00013 avg:5.100000 max:20 min:-10",
		"country:country-00014 avg:6.100000 max:21 min:-9",
		"country:country-00015 avg:7.100000 max:22 min:-8",
		"country:country-00016 avg:8.100000 max:23 min:-7",
		"country:country-00017 avg:9.100000 max:24 min:-6",
		"country:country-00018 avg:5.200000 max:20 min:-10",
		"country:country-00019 avg:6.200000 max:21 min:-9"}

	waitForRowsWithVerifier(t, fmt.Sprintf("(scan all from %s) -> (sort by country)", tableName),
		func(qr client.QueryResult) bool {
			for i := 0; i < qr.RowCount(); i++ {
				row := qr.Row(i)
				country := row.StringVal(1)
				avgTemp := row.FloatVal(2)
				maxTemp := row.IntVal(3)
				minTemp := row.IntVal(4)
				line := fmt.Sprintf("country:%s avg:%f max:%d min:%d", country, avgTemp, maxTemp, minTemp)
				exp := expected[i]
				if exp != line {
					log.Errorf("%s: expected line: %s got: %s", t.Name(), exp, line)
					return false
				}
			}
			log.Debugf("waited and got %d rows", qr.RowCount())
			return qr.RowCount() == numRows
		}, cl)
}

func waitForRowsWithVerifier(t *testing.T, query string, verifier func(client.QueryResult) bool, cl client.Client) {
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		log.Debug("************ waiting for rows")
		qr, err := cl.ExecuteQuery(query)
		if err != nil {
			return false, err
		}
		return verifier(qr), nil
	}, 10*time.Second, 1*time.Second)
	require.NoError(t, err)
	require.True(t, ok)
}

func setupServers(t *testing.T, fk *fake.Kafka) ([]*server.Server, func(t *testing.T)) {
	common.SetGRDebug(true)
	common.SetTimerDebug(true)
	return startClusterWithConfigSetter(t, 3, fk, func(cfg *conf.Config) {
		cfg.KafkaServerEnabled = true
		var kafkaListenAddresses []string
		for i := 0; i < 3; i++ {
			address, err := common.AddressWithPort("localhost")
			require.NoError(t, err)
			kafkaListenAddresses = append(kafkaListenAddresses, address)
		}
		cfg.KafkaServerListenerConfig.Addresses = kafkaListenAddresses
		cfg.MinSnapshotInterval = 1 * time.Second
		// Make sure we push frequently to exercise logic on level manager with dead versions
		cfg.MemtableMaxReplaceInterval = 1 * time.Second
		cfg.CompactionWorkersEnabled = true
		// We set compaction timeout to a low value - when a node dies, pending jobs it owns will need to be timed out
		// and before they time out compaction can stall, resulting in L0 reaching max size and shutdown not completing
		cfg.CompactionJobTimeout = 5 * time.Second
		cfg.KafkaInitialJoinDelay = 10 * time.Millisecond // to speed tests up

		cfg.LevelManagerRetryDelay = 1 * time.Second
	})
}

func createClient(t *testing.T, node int, servers []*server.Server) client.Client {
	clientTLSConfig := client.TLSConfig{
		TrustedCertsPath: serverCertPath,
	}
	client, err := client.NewClient(servers[node].GetConfig().HttpApiAddresses[node], clientTLSConfig)
	require.NoError(t, err)
	return client
}

func getFailAndClientNodes(failLevelManager bool, servers []*server.Server) (int, int) {
	levelManagerNode := getLevelManagerNode(servers)
	var failNode int
	if failLevelManager {
		failNode = levelManagerNode
	} else {
		failNode = (levelManagerNode + 1) % 3
	}
	clientNode := (failNode + 1) % 3
	return levelManagerNode, clientNode
}

func getLevelManagerNode(servers []*server.Server) int {
	for i, s := range servers {
		if s.GetLevelManager() != nil {
			return i
		}
	}
	panic("cannot find level manager")
}

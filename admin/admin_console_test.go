package admin

import (
	"fmt"
	"github.com/spirit-labs/tektite/clustmgr"
	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/conf"
	"github.com/spirit-labs/tektite/levels"
	"github.com/spirit-labs/tektite/mem"
	"github.com/spirit-labs/tektite/opers"
	"github.com/spirit-labs/tektite/parser"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/remoting"
	"github.com/spirit-labs/tektite/vmgr"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"testing"
	"time"
)

const (
	serverKeyPath  = "testdata/serverkey.pem"
	serverCertPath = "testdata/servercert.pem"
)

func init() {
	common.EnableTestPorts()
}

func TestGetDatabaseStats(t *testing.T) {
	var stats levels.Stats
	stats.TotBytes = 2323232323
	stats.TotEntries = 46545454
	stats.TotTables = 343343
	stats.BytesIn = 7777777
	stats.EntriesIn = 555555
	stats.TablesIn = 4444444
	stats.LevelStats = map[int]*levels.LevelStats{
		0: {
			Bytes:   333343433,
			Entries: 2222,
			Tables:  1111,
		},
		1: {
			Bytes:   45645677456,
			Entries: 555,
			Tables:  5444,
		},
		2: {
			Bytes:   8888887878787878,
			Entries: 777,
			Tables:  444,
		},
	}

	expected :=
		`<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;}</style>
<title>Database Stats</title>
</head>
<body>
<h1>Database Stats</h1>

<table border="1" width="50%">
<tr>
	<th width="35%">Total size</th>
	<th>Total table count</th>
	<th>Total entry count</th>
</tr>
<tr>
	<td>2.16 GiB (2323232323 bytes)</td>
	<td>343343</td>
	<td>46545454</td>
</tr>
</table>

<br></br>

<table border="1" width="50%">
<tr>
	<th width="35%">Total bytes in</th>
	<th>Total tables in</th>
	<th>Total entries in</th>
</tr>
<tr>
	<td>7.42 MiB (7777777 bytes)</td>
	<td>4444444</td>
	<td>555555</td>
</tr>
</table>

<br></br>

<table border="1" width="50%">
<tr>
	<th>Level</th>
	<th>Size</th>
	<th>Table count</th>
	<th>Entry count</th>
</tr>

<tr>
	<td>0</td>
	<td>317.90 MiB (333343433 bytes)</td>
	<td>1111</td>
	<td>2222</td>
</tr>

<tr>
	<td>1</td>
	<td>42.51 GiB (45645677456 bytes)</td>
	<td>5444</td>
	<td>555</td>
</tr>

<tr>
	<td>2</td>
	<td>7.89 PiB (8888887878787878 bytes)</td>
	<td>444</td>
	<td>777</td>
</tr>

</table>

</body>
</html>
`
	testAdminConsole(t, "database", stats, nil, nil, nil, nil, expected)
}

func TestTopics(t *testing.T) {

	expected :=
		`<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;}</style>
<title>Tektite Topics</title>
</head>
<body>
<h1>Topics</h1>

<table border="1" width="50%">
<tr>
	<th>Name</th>
	<th>Partitions</th>
	<th>Read/Write</th>
</tr>

<tr>
	<td>topic_ro</td>
	<td>43</td>
	<td>r</td>
</tr>

<tr>
	<td>topic_rw</td>
	<td>23</td>
	<td>rw</td>
</tr>

<tr>
	<td>topic_wo</td>
	<td>77</td>
	<td>w</td>
</tr>

</table>

</body>
</html>
`
	kafkaEndpoints := []*opers.KafkaEndpointInfo{
		{
			Name:        "topic_rw",
			InEndpoint:  &opers.KafkaInOperator{},
			OutEndpoint: &opers.KafkaOutOperator{},
			Schema: &opers.OperatorSchema{
				PartitionScheme: opers.PartitionScheme{
					Partitions: 23,
				},
			},
		},
		{
			Name:        "topic_ro",
			OutEndpoint: &opers.KafkaOutOperator{},
			Schema: &opers.OperatorSchema{
				PartitionScheme: opers.PartitionScheme{
					Partitions: 43,
				},
			},
		},
		{
			Name:       "topic_wo",
			InEndpoint: &opers.KafkaInOperator{},
			Schema: &opers.OperatorSchema{
				PartitionScheme: opers.PartitionScheme{
					Partitions: 77,
				},
			},
		},
	}
	testAdminConsole(t, "topics", levels.Stats{}, kafkaEndpoints, nil, nil, nil, expected)
}

func TestStreams(t *testing.T) {

	expected :=
		`<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;}</style>
<title>Tektite Streams</title>
</head>
<body>
<h1>Streams</h1>
<table border="1" width="100%">
<tr>
	<th>Name</th>
	<th width="15%">Definition</th>
	<th width="15%">In Schema</th>
	<th>In Partitions</th>
	<th>In Mapping</th>
	<th width="15%">Out Schema</th>
	<th>Out Partitions</th>
	<th>Out Mapping</th>
	<th>Child Streams</th>
</tr>

<tr>
	<td>child_stream_a</td>
	<td>stream_abc -&gt; (to stream)</td>
	<td>offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes</td>
	<td>23</td>
	<td>mapping1</td>
	<td>offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes</td>
	<td>23</td>
	<td>mapping1</td>
	<td></td>
</tr>

<tr>
	<td>child_stream_b</td>
	<td>stream_abc -&gt; (to stream)</td>
	<td>offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes</td>
	<td>23</td>
	<td>mapping1</td>
	<td>offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes</td>
	<td>23</td>
	<td>mapping1</td>
	<td></td>
</tr>

<tr>
	<td>stream_abc</td>
	<td>(bridge from topicA partitions=10)-&gt;(to stream)</td>
	<td>offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes</td>
	<td>23</td>
	<td>mapping1</td>
	<td>offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes</td>
	<td>23</td>
	<td>mapping1</td>
	<td>child_stream_a, child_stream_ab</td>
</tr>

<tr>
	<td>stream_xyz</td>
	<td>(bridge from topicA partitions=10)-&gt;(partition by key partitions=20)-&gt;(to stream)</td>
	<td>offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes</td>
	<td>23</td>
	<td>mapping1</td>
	<td>offset: int, event_time: timestamp, key: bytes, hdrs: bytes, val: bytes</td>
	<td>20</td>
	<td>mapping2</td>
	<td></td>
</tr>

</table>
</body>
</html>
`
	streamInfos := []*opers.StreamInfo{
		{
			StreamDesc: parser.CreateStreamDesc{
				StreamName: "sys_stream1",
			},
			SystemStream: true,
		},
		{
			StreamDesc: parser.CreateStreamDesc{
				StreamName: "stream_abc",
			},
			DownstreamStreamNames: map[string]struct{}{
				"child_stream_a":  {},
				"child_stream_ab": {},
			},
			Tsl: "stream_abc := (bridge from topicA partitions=10)->(to stream)",
			InSchema: &opers.OperatorSchema{
				EventSchema: opers.KafkaSchema,
				PartitionScheme: opers.PartitionScheme{
					Partitions: 23,
					MappingID:  "mapping1",
				},
			},
			OutSchema: &opers.OperatorSchema{
				EventSchema: opers.KafkaSchema,
				PartitionScheme: opers.PartitionScheme{
					Partitions: 23,
					MappingID:  "mapping1",
				},
			},
			SystemStream: false,
		},
		{
			StreamDesc: parser.CreateStreamDesc{
				StreamName: "stream_xyz",
			},
			DownstreamStreamNames: nil,
			Tsl:                   "stream_xyz := (bridge from topicA partitions=10)->(partition by key partitions=20)->(to stream)",
			InSchema: &opers.OperatorSchema{
				EventSchema: opers.KafkaSchema,
				PartitionScheme: opers.PartitionScheme{
					Partitions: 23,
					MappingID:  "mapping1",
				},
			},
			OutSchema: &opers.OperatorSchema{
				EventSchema: opers.KafkaSchema,
				PartitionScheme: opers.PartitionScheme{
					Partitions: 20,
					MappingID:  "mapping2",
				},
			},
			SystemStream: false,
		},
		{
			StreamDesc: parser.CreateStreamDesc{
				StreamName: "child_stream_a",
			},
			DownstreamStreamNames: nil,
			Tsl:                   "child_stream_a := stream_abc -> (to stream)",
			InSchema: &opers.OperatorSchema{
				EventSchema: opers.KafkaSchema,
				PartitionScheme: opers.PartitionScheme{
					Partitions: 23,
					MappingID:  "mapping1",
				},
			},
			OutSchema: &opers.OperatorSchema{
				EventSchema: opers.KafkaSchema,
				PartitionScheme: opers.PartitionScheme{
					Partitions: 23,
					MappingID:  "mapping1",
				},
			},
			SystemStream: false,
		},
		{
			StreamDesc: parser.CreateStreamDesc{
				StreamName: "child_stream_b",
			},
			DownstreamStreamNames: nil,
			Tsl:                   "child_stream_b := stream_abc -> (to stream)",
			InSchema: &opers.OperatorSchema{
				EventSchema: opers.KafkaSchema,
				PartitionScheme: opers.PartitionScheme{
					Partitions: 23,
					MappingID:  "mapping1",
				},
			},
			OutSchema: &opers.OperatorSchema{
				EventSchema: opers.KafkaSchema,
				PartitionScheme: opers.PartitionScheme{
					Partitions: 23,
					MappingID:  "mapping1",
				},
			},
			SystemStream: false,
		},
	}

	testAdminConsole(t, "streams", levels.Stats{}, nil, streamInfos, nil, nil, expected)
}

func TestConfig(t *testing.T) {

	expected :=
		`<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;} pre {font-family: 'Consolas', monospace;white-space: pre-wrap;}</style>
<title>Tektite Config</title>
</head>
<body>
<h1>Config</h1>
<pre>
// Config for a standalone one node Tektite server - useful for development purposes

processor-count = 16

processing-enabled = true
level-manager-enabled = true
compaction-workers-enabled = true

l0-compaction-trigger = 4
l0-max-tables-before-blocking = 10

cluster-addresses = [&#34;localhost:63301&#34;]

http-api-enabled = true
http-api-addresses  = [&#34;localhost:7770&#34;]
http-api-tls-key-path = &#34;cfg/certs/server.key&#34;
http-api-tls-cert-path = &#34;cfg/certs/server.crt&#34;

kafka-server-enabled = true
kafka-server-addresses  = [&#34;localhost:8880&#34;]

web-ui-enabled = true
web-ui-listen-addresses =  [&#34;localhost:9990&#34;]

object-store-type = &#34;minio&#34;
minio-endpoint = &#34;127.0.0.1:9000&#34;
minio-access-key = &#34;0UC0OHDZSrargDyctdur&#34;
minio-secret-key = &#34;MZ9xPPiNuOGssOnAjHaUvRLShG3gkQZmf43QvhXB&#34;
minio-bucket-name = &#34;tektite-dev&#34;

// Logging config
log-level = &#34;info&#34;
log-format = &#34;console&#34;
</pre>
</body>
</html>
`

	cfgOrig := `
// Config for a standalone one node Tektite server - useful for development purposes

processor-count = 16

processing-enabled = true
level-manager-enabled = true
compaction-workers-enabled = true

l0-compaction-trigger = 4
l0-max-tables-before-blocking = 10

cluster-addresses = ["localhost:63301"]

http-api-enabled = true
http-api-addresses  = ["localhost:7770"]
http-api-tls-key-path = "cfg/certs/server.key"
http-api-tls-cert-path = "cfg/certs/server.crt"

kafka-server-enabled = true
kafka-server-addresses  = ["localhost:8880"]

web-ui-enabled = true
web-ui-listen-addresses =  ["localhost:9990"]

object-store-type = "minio"
minio-endpoint = "127.0.0.1:9000"
minio-access-key = "0UC0OHDZSrargDyctdur"
minio-secret-key = "MZ9xPPiNuOGssOnAjHaUvRLShG3gkQZmf43QvhXB"
minio-bucket-name = "tektite-dev"

// Logging config
log-level = "info"
log-format = "console"
`
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.HttpApiTlsConfig = conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}
	cfg.Original = cfgOrig

	testAdminConsole(t, "config", levels.Stats{}, nil, nil, cfg, nil, expected)
}

func TestCluster(t *testing.T) {
	groupStates := map[int]clustmgr.GroupState{
		0: {
			GroupNodes: []clustmgr.GroupNode{
				{
					NodeID: 0,
					Leader: true,
					Valid:  true,
				},
				{
					NodeID: 1,
					Leader: false,
					Valid:  true,
				},
				{
					NodeID: 2,
					Leader: false,
					Valid:  false,
				},
			},
		},
		1: {
			GroupNodes: []clustmgr.GroupNode{
				{
					NodeID: 0,
					Leader: false,
					Valid:  true,
				},
				{
					NodeID: 1,
					Leader: true,
					Valid:  true,
				},
				{
					NodeID: 2,
					Leader: false,
					Valid:  true,
				},
			},
		},
		2: {
			GroupNodes: []clustmgr.GroupNode{
				{
					NodeID: 0,
					Leader: false,
					Valid:  true,
				},
				{
					NodeID: 1,
					Leader: false,
					Valid:  true,
				},
				{
					NodeID: 2,
					Leader: true,
					Valid:  true,
				},
			},
		},
	}
	expected := `<html>
<head>
<link href='https://fonts.googleapis.com/css?family=Roboto:400' rel='stylesheet' type='text/css'>
<style>body {font-family: 'Roboto', sans-serif;} pre {font-family: 'Consolas', monospace;white-space: pre-wrap;}</style>
<title>Tektite Cluster State</title>
</head>
<body>
<h1>Cluster State</h1>
Node count: 3<br></br>
Live nodes: [0 1 2]<br></br>
<br></br>

	<h3>Node id: 0</h3><br></br>
	Leader count: 1<br></br>
	Replica Count: 3<br></br>
	<table border="1" width="100%">
		<tr>
			<th>Processor ID</th>
			<th>Live?</th>
			<th>Backup?</th>
			<th>Synced?</th>
		</tr>
		
			<tr>
				<td>0</td>
				<td>true</td>
				<td>false</td>
				<td>true</td>
			</tr>
		
			<tr>
				<td>1</td>
				<td>false</td>
				<td>true</td>
				<td>true</td>
			</tr>
		
			<tr>
				<td>2</td>
				<td>false</td>
				<td>true</td>
				<td>true</td>
			</tr>
		
	</table>

	<h3>Node id: 1</h3><br></br>
	Leader count: 1<br></br>
	Replica Count: 3<br></br>
	<table border="1" width="100%">
		<tr>
			<th>Processor ID</th>
			<th>Live?</th>
			<th>Backup?</th>
			<th>Synced?</th>
		</tr>
		
			<tr>
				<td>0</td>
				<td>false</td>
				<td>true</td>
				<td>true</td>
			</tr>
		
			<tr>
				<td>1</td>
				<td>true</td>
				<td>false</td>
				<td>true</td>
			</tr>
		
			<tr>
				<td>2</td>
				<td>false</td>
				<td>true</td>
				<td>true</td>
			</tr>
		
	</table>

	<h3>Node id: 2</h3><br></br>
	Leader count: 1<br></br>
	Replica Count: 3<br></br>
	<table border="1" width="100%">
		<tr>
			<th>Processor ID</th>
			<th>Live?</th>
			<th>Backup?</th>
			<th>Synced?</th>
		</tr>
		
			<tr>
				<td>0</td>
				<td>false</td>
				<td>true</td>
				<td>false</td>
			</tr>
		
			<tr>
				<td>1</td>
				<td>false</td>
				<td>true</td>
				<td>true</td>
			</tr>
		
			<tr>
				<td>2</td>
				<td>true</td>
				<td>false</td>
				<td>true</td>
			</tr>
		
	</table>

</body>
</html>`
	cfg := &conf.Config{}
	cfg.ApplyDefaults()
	cfg.HttpApiTlsConfig = conf.TLSConfig{
		Enabled:  true,
		KeyPath:  serverKeyPath,
		CertPath: serverCertPath,
	}
	cfg.ClusterAddresses = []string{"localhost:44400", "localhost:44401", "localhost:44402"}
	testAdminConsole(t, "cluster", levels.Stats{}, nil, nil, cfg, groupStates, expected)
}

func testAdminConsole(t *testing.T, path string, stats levels.Stats, kafkaEndpoints []*opers.KafkaEndpointInfo,
	allStreams []*opers.StreamInfo, cfg *conf.Config, groupStates map[int]clustmgr.GroupState, expected string) {

	if cfg == nil {
		cfg = &conf.Config{}
		cfg.ApplyDefaults()
	}

	address, err := common.AddressWithPort("localhost")
	require.NoError(t, err)
	cfg.AdminConsoleEnabled = true
	cfg.AdminConsoleAddresses = []string{address}

	levelMgrClient := &testLevelMgrClient{}
	levelMgrClient.stats = stats

	streamManager := &testStreamManager{}
	streamManager.kafkaEndpoints = kafkaEndpoints
	streamManager.allStreams = allStreams

	procMgr := &testProcessorManager{}
	procMgr.groupStates = groupStates

	webui, err := NewServer(cfg, levelMgrClient, streamManager, procMgr)
	require.NoError(t, err)

	err = webui.Start()
	require.NoError(t, err)
	defer func() {
		err := webui.Stop()
		require.NoError(t, err)
	}()

	uri := fmt.Sprintf("http://%s/%s", address, path)

	client := createClient(t)

	resp, err := client.Get(uri)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, expected, string(body))
}

func createClient(t *testing.T) *http.Client {
	t.Helper()
	return &http.Client{}
}

type testLevelMgrClient struct {
	stats levels.Stats
}

func (t *testLevelMgrClient) GetTableIDsForRange(keyStart []byte, keyEnd []byte) (levels.OverlappingTableIDs, []levels.VersionRange, error) {
	return nil, nil, nil
}

func (t *testLevelMgrClient) RegisterL0Tables(levels.RegistrationBatch) error {
	return nil
}

func (t *testLevelMgrClient) ApplyChanges(levels.RegistrationBatch) error {
	return nil
}

func (t *testLevelMgrClient) PollForJob() (*levels.CompactionJob, error) {
	return nil, nil
}

func (t *testLevelMgrClient) RegisterDeadVersionRange(levels.VersionRange, string, int) error {
	return nil
}

func (t *testLevelMgrClient) StoreLastFlushedVersion(int64) error {
	return nil
}

func (t *testLevelMgrClient) LoadLastFlushedVersion() (int64, error) {
	return 0, nil
}

func (t *testLevelMgrClient) GetStats() (levels.Stats, error) {
	return t.stats, nil
}

func (t *testLevelMgrClient) RegisterSlabRetention(slabID int, retention time.Duration) error {
	return nil
}

func (t *testLevelMgrClient) UnregisterSlabRetention(slabID int) error {
	return nil
}

func (t *testLevelMgrClient) GetSlabRetention(slabID int) (time.Duration, error) {
	return 0, nil
}

func (t *testLevelMgrClient) Start() error {
	return nil
}

func (t *testLevelMgrClient) Stop() error {
	panic("not implemented")
}

type testStreamManager struct {
	kafkaEndpoints []*opers.KafkaEndpointInfo
	allStreams     []*opers.StreamInfo
}

func (t *testStreamManager) HandleProcessBatch(proc.Processor, *proc.ProcessBatch, bool) (bool, *mem.Batch, []*proc.ProcessBatch, error) {
	panic("not implemented")
}

func (t *testStreamManager) GetForwardingProcessorCount(int) (int, bool) {
	panic("not implemented")
}

func (t *testStreamManager) GetInjectableReceivers(int) []int {
	panic("not implemented")
}

func (t *testStreamManager) GetRequiredCompletions() int {
	panic("not implemented")
}

func (t *testStreamManager) GetTerminalReceiverCount() int {
	panic("not implemented")
}

func (t *testStreamManager) DeployStream(parser.CreateStreamDesc, []int, []int, string, int64) error {
	panic("not implemented")
}

func (t *testStreamManager) UndeployStream(parser.DeleteStreamDesc, int64) error {
	panic("not implemented")
}

func (t *testStreamManager) GetStream(string) *opers.StreamInfo {
	panic("not implemented")
}

func (t *testStreamManager) GetAllStreams() []*opers.StreamInfo {
	return t.allStreams
}

func (t *testStreamManager) GetKafkaEndpoint(string) *opers.KafkaEndpointInfo {
	panic("not implemented")
}

func (t *testStreamManager) GetAllKafkaEndpoints() []*opers.KafkaEndpointInfo {
	return t.kafkaEndpoints
}

func (t *testStreamManager) RegisterSystemSlab(string, int, int, int, *opers.OperatorSchema, []string, bool) error {
	panic("not implemented")
}

func (t *testStreamManager) SetProcessorManager(opers.ProcessorManager) {
	panic("not implemented")
}

func (t *testStreamManager) GetIngestedMessageCount() int {
	panic("not implemented")
}

func (t *testStreamManager) PrepareForShutdown() {
	panic("not implemented")
}

func (t *testStreamManager) StreamCount() int {
	panic("not implemented")
}

func (t *testStreamManager) Start() error {
	panic("not implemented")
}

func (t *testStreamManager) Stop() error {
	panic("not implemented")
}

func (t *testStreamManager) Loaded() {
	panic("not implemented")
}

func (t *testStreamManager) RegisterChangeListener(func(streamName string, deployed bool)) {
	panic("not implemented")
}

func (t *testStreamManager) RegisterReceiver() {
	panic("not implemented")
}

func (t *testStreamManager) RegisterReceiverWithLock(int, opers.Receiver) {
	panic("not implemented")
}

func (t *testStreamManager) StartIngest(int) error {
	panic("not implemented")
}

func (t *testStreamManager) StopIngest() error {
	panic("not implemented")
}

func (t *testStreamManager) StreamMetaIteratorProvider() *opers.StreamMetaIteratorProvider {
	panic("not implemented")
}

func (t *testStreamManager) Dump() {
	panic("not implemented")
}

type testProcessorManager struct {
	groupStates map[int]clustmgr.GroupState
}

func (t *testProcessorManager) ClearUnflushedData() {
	panic("not implemented")
}

func (t *testProcessorManager) FlushAllProcessors(shutdown bool) error {
	panic("not implemented")
}

func (t *testProcessorManager) GetLastCompletedVersion() int64 {
	panic("not implemented")
}

func (t *testProcessorManager) GetLastFlushedVersion() int64 {
	panic("not implemented")
}

func (t *testProcessorManager) RegisterStateHandler(clustmgr.ClusterStateHandler) {
	panic("not implemented")
}

func (t *testProcessorManager) NodePartitions(string, int) (map[int][]int, error) {
	panic("not implemented")
}

func (t *testProcessorManager) NodeForPartition(int, string, int) int {
	panic("not implemented")
}

func (t *testProcessorManager) ForwardBatch(*proc.ProcessBatch, bool, func(error)) {
	panic("not implemented")
}

func (t *testProcessorManager) Start() error {
	panic("not implemented")
}

func (t *testProcessorManager) Stop() error {
	panic("not implemented")
}

func (t *testProcessorManager) RegisterListener(string, proc.ProcessorListener) []proc.Processor {
	panic("not implemented")
}

func (t *testProcessorManager) UnregisterListener(string) {
	panic("not implemented")
}

func (t *testProcessorManager) SetClusterMessageHandlers(remoting.Server, *remoting.TeeBlockingClusterMessageHandler) {
	panic("not implemented")
}

func (t *testProcessorManager) MarkGroupAsValid(int, int, int) (bool, error) {
	panic("not implemented")
}

func (t *testProcessorManager) SetVersionManagerClient(vmgr.Client) {
	panic("not implemented")
}

func (t *testProcessorManager) HandleVersionBroadcast(int, int, int) {
	panic("not implemented")
}

func (t *testProcessorManager) GetGroupState(processorID int) (clustmgr.GroupState, bool) {
	gs, ok := t.groupStates[processorID]
	return gs, ok
}

func (t *testProcessorManager) GetLeaderNode(int) (int, error) {
	panic("not implemented")
}

func (t *testProcessorManager) GetProcessor(int) proc.Processor {
	panic("not implemented")
}

func (t *testProcessorManager) ClusterVersion() int {
	panic("not implemented")
}

func (t *testProcessorManager) IsReadyAsOfVersion(int) bool {
	panic("not implemented")
}

func (t *testProcessorManager) HandleClusterState(clustmgr.ClusterState) error {
	panic("not implemented")
}

func (t *testProcessorManager) AfterReceiverChange() {
	panic("not implemented")
}

func (t *testProcessorManager) GetCurrentVersion() int {
	panic("not implemented")
}

func (t *testProcessorManager) PrepareForShutdown() {
	panic("not implemented")
}

func (t *testProcessorManager) AcquiesceLevelManagerProcessor() error {
	panic("not implemented")
}

func (t *testProcessorManager) WaitForProcessingToComplete() {
	panic("not implemented")
}

func (t *testProcessorManager) Freeze() {
	panic("not implemented")
}

func (t *testProcessorManager) FailoverOccurred() bool {
	panic("not implemented")
}

func (t *testProcessorManager) VersionManagerClient() vmgr.Client {
	panic("not implemented")
}

func (t *testProcessorManager) EnsureReplicatorsReady() error {
	panic("not implemented")
}

func (t *testProcessorManager) SetLevelMgrProcessorInitialisedCallback(func() error) {
	panic("not implemented")
}

func TestFormatBytes(t *testing.T) {
	require.Equal(t, "0 bytes", formatBytes(0))
	require.Equal(t, "100 bytes", formatBytes(100))
	require.Equal(t, "1023 bytes", formatBytes(1023))
	require.Equal(t, "1.00 KiB (1024 bytes)", formatBytes(1024))
	require.Equal(t, "1.00 MiB (1048576 bytes)", formatBytes(1024*1024))
	require.Equal(t, "1.00 GiB (1073741824 bytes)", formatBytes(1024*1024*1024))
	require.Equal(t, "1.00 TiB (1099511627776 bytes)", formatBytes(1024*1024*1024*1024))
	require.Equal(t, "1.00 PiB (1125899906842624 bytes)", formatBytes(1024*1024*1024*1024*1024))
}

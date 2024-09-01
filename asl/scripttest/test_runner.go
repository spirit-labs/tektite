package scripttest

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	kafkago "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/spirit-labs/tektite/asl/cli"
	"github.com/spirit-labs/tektite/asl/conf"
	"github.com/spirit-labs/tektite/asl/errwrap"
	"github.com/spirit-labs/tektite/asl/server"
	"github.com/spirit-labs/tektite/client"
	"github.com/spirit-labs/tektite/kafka/fake"
	"github.com/spirit-labs/tektite/objstore/dev"
	"github.com/spirit-labs/tektite/proc"
	"github.com/spirit-labs/tektite/protos/clustermsgs"
	"github.com/spirit-labs/tektite/shutdown"
	"github.com/spirit-labs/tektite/testutils"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	log "github.com/spirit-labs/tektite/logger"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/spirit-labs/tektite/common"
	"github.com/spirit-labs/tektite/kafka"
	_ "net/http/pprof" //nolint:gosec
)

const (
	TestPrefixes           = "" // Set this to a prefix of a test if you want to only run those tests, e.g. during development
	ExcludedTestPrefixes   = ""
	caSignedServerKeyPath  = "testdata/keys/casignedserverkey.pem"
	caSignedServerCertPath = "testdata/keys/casignedservercert.pem"
)

const (
	clientPageSize      = 3 // We set this to a small value to exercise the paging logic
	useMinioObjectStore = false
)

func init() {
	common.EnableTestPorts()
}

type scriptTestSuite struct {
	numNodes               int
	replicationFactor      int
	useHTTPAPI             bool
	intraClusterTLSEnabled bool
	tlsKeysInfo            *TLSKeysInfo
	etcdAddress            string
	fakeKafka              *fake.Kafka
	tektiteCluster         []*server.Server
	suite                  suite.Suite
	tests                  map[string]*scriptTest
	t                      *testing.T
	lock                   sync.Mutex
	objectStoreType        string
	devObjStoreAddress     string
	devObjStore            *dev.Store
	versionLock            sync.Mutex
	latestVersion          *clustermsgs.VersionsMessage
	testName               string
}

type TLSKeysInfo struct {
	ServerCertPath       string
	ServerKeyPath        string
	ClientCertPath       string
	ClientKeyPath        string
	IntraClusterCertPath string
	IntraClusterKeyPath  string
}

func (w *scriptTestSuite) T() *testing.T {
	return w.suite.T()
}

func (w *scriptTestSuite) SetT(t *testing.T) {
	t.Helper()
	w.suite.SetT(t)
}

func (w *scriptTestSuite) SetS(suite suite.TestingSuite) {
	w.suite.SetS(suite)
}

func TestScript(t *testing.T, numNodes int, replicationFactor int,
	intraClusterTLSEnabled bool, tlsKeysInfo *TLSKeysInfo, etcdAddress string) {
	t.Helper()

	common.RequireDebugServer(t)

	// we need to give each test a unique etcd prefix so they don't interfere with other tests using etcd in parallel
	ts := &scriptTestSuite{tests: make(map[string]*scriptTest), t: t, testName: t.Name()}
	ts.setup(numNodes, replicationFactor, intraClusterTLSEnabled, tlsKeysInfo, etcdAddress)

	defer ts.teardown()

	suite.Run(t, ts)
}

func (w *scriptTestSuite) TestScript() {
	for testName, sTest := range w.tests {
		w.suite.Run(testName, sTest.run)
	}
}

func (w *scriptTestSuite) setupTektiteCluster() {
	confs := w.createServerConfs()
	w.startCluster(confs)
}

func (w *scriptTestSuite) createServerConfs() []conf.Config {
	serverTLSConfig := conf.TLSConfig{
		Enabled:  true,
		KeyPath:  caSignedServerKeyPath,
		CertPath: caSignedServerCertPath,
	}

	var remotingAddresses []string
	var httpServerListenAddresses []string
	var kafkaListenAddresses []string
	for i := 0; i < w.numNodes; i++ {
		kafkaAddress, err := common.AddressWithPort("localhost")
		if err != nil {
			log.Fatal(err)
		}
		kafkaListenAddresses = append(kafkaListenAddresses, kafkaAddress)
		remotingAddress, err := common.AddressWithPort("localhost")
		if err != nil {
			log.Fatal(err)
		}
		remotingAddresses = append(remotingAddresses, remotingAddress)
		apiAddress, err := common.AddressWithPort("localhost")
		if err != nil {
			log.Fatal(err)
		}
		httpServerListenAddresses = append(httpServerListenAddresses, apiAddress)
	}

	cfg := conf.Config{}
	cfg.ApplyDefaults()
	cfg.ClusterName = w.testName
	cfg.ClusterAddresses = remotingAddresses
	cfg.HttpApiEnabled = true
	cfg.HttpApiAddresses = httpServerListenAddresses
	cfg.HttpApiTlsConfig = serverTLSConfig
	cfg.KafkaServerListenerConfig.Addresses = kafkaListenAddresses
	cfg.KafkaServerEnabled = true
	cfg.ProcessingEnabled = true
	cfg.LevelManagerEnabled = true
	// We set snapshots to complete fast, so we minimise delay waiting for new version after
	// loading data
	cfg.MinSnapshotInterval = 10 * time.Millisecond
	// We set initial join delay to a short value so we don't have long delays in consumer tests
	cfg.KafkaInitialJoinDelay = 10 * time.Millisecond
	cfg.MaxBackfillBatchSize = 3 // To exercise multiple batches loaded in back-fill
	cfg.CompactionWorkersEnabled = true
	cfg.CompactionWorkerCount = 4
	cfg.CommandCompactionInterval = 5 * time.Second
	// In real life don't want to set this so low otherwise cluster state will be calculated when just one node
	// is started with all leaders
	cfg.ClusterStateUpdateInterval = 10 * time.Millisecond
	cfg.ClusterManagerAddresses = []string{w.etcdAddress}

	// Set this low so store retries quickly to get prefix retentions on startup.
	cfg.LevelManagerRetryDelay = 10 * time.Millisecond

	cfg.ObjectStoreType = w.objectStoreType
	if w.objectStoreType == conf.MinioObjectStoreType {
		cfg.ObjectStoreType = conf.MinioObjectStoreType
		cfg.MinioEndpoint = "127.0.0.1:9000"
		cfg.MinioAccessKey = "tYTKoueu7NyentYPe3OF"
		cfg.MinioSecretKey = "DxMe9mGt5OEeUNvqv3euXMcOx7mmLui6g9q4CMjB"
		cfg.MinioBucketName = "tektite-dev"
	} else if w.objectStoreType == conf.DevObjectStoreType {
		cfg.DevObjectStoreAddresses = []string{w.devObjStoreAddress}
	}

	cfg.LogScope = w.testName

	var serverConfs []conf.Config
	for i := 0; i < w.numNodes; i++ {
		cfgCopy := cfg
		cfgCopy.NodeID = i
		serverConfs = append(serverConfs, cfgCopy)
	}
	return serverConfs
}

func (w *scriptTestSuite) startCluster(confs []conf.Config) {
	var servers []*server.Server
	var chans []chan error
	for _, cfg := range confs {
		s, err := server.NewServerWithClientFactory(cfg, fake.NewFakeMessageClientFactory(w.fakeKafka))
		if err != nil {
			panic(err)
		}
		servers = append(servers, s)
		ch := make(chan error, 1)
		chans = append(chans, ch)
		theServer := s
		go func() {
			err := theServer.Start()
			ch <- err
		}()
	}

	for _, ch := range chans {
		err := <-ch
		if err != nil {
			panic(err)
		}
	}

	w.tektiteCluster = servers
}

func (w *scriptTestSuite) setup(numNodes int, replicationFactor int, intraClusterTLSEnabled bool,
	tlsKeysInfo *TLSKeysInfo, etcdAddress string) {
	w.numNodes = numNodes
	w.replicationFactor = replicationFactor
	w.intraClusterTLSEnabled = intraClusterTLSEnabled
	w.tlsKeysInfo = tlsKeysInfo
	w.etcdAddress = etcdAddress
	w.fakeKafka = &fake.Kafka{}

	if useMinioObjectStore {
		w.objectStoreType = conf.MinioObjectStoreType
	} else {
		w.objectStoreType = conf.DevObjectStoreType
		devStoreObjAddress, err := common.AddressWithPort("localhost")
		if err != nil {
			log.Fatal(err)
		}
		w.devObjStoreAddress = devStoreObjAddress
		w.devObjStore = dev.NewDevStore(w.devObjStoreAddress)
		if err := w.devObjStore.Start(); err != nil {
			panic(err)
		}
	}

	w.setupTektiteCluster()

	files, err := os.ReadDir("./testdata")
	if err != nil {
		log.Fatal(err)
	}
	sort.SliceStable(files, func(i, j int) bool {
		return strings.Compare(files[i].Name(), files[j].Name()) < 0
	})
	currTestName := ""
	currSQLTest := &scriptTest{}
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		fileName := file.Name()
		if //goland:noinspection GoBoolExpressions
		TestPrefixes != "" {
			prefixes := strings.Split(TestPrefixes, ",")
			exclude := true
			for _, prefix := range prefixes {
				if strings.HasPrefix(fileName, prefix) {
					exclude = false
					break
				}
			}
			if exclude {
				continue
			}
		}
		if //goland:noinspection GoBoolExpressions
		ExcludedTestPrefixes != "" {
			exclude := false
			excluded := strings.Split(ExcludedTestPrefixes, ",")
			for _, excl := range excluded {
				if strings.HasPrefix(fileName, excl) {
					exclude = true
					break
				}
			}
			if exclude {
				continue
			}
		}
		if !strings.HasSuffix(fileName, "_test_data.txt") && !strings.HasSuffix(fileName, "_test_out.txt") && !strings.HasSuffix(fileName, "_test_script.txt") {
			log.Fatalf("test file %s has invalid name. test files should be of the form <test_name>_test_data.txt, <test_name>_test_script.txt or <test_name>_test_out.txt,", fileName)
		}
		if (currTestName == "") || !strings.HasPrefix(fileName, currTestName) {
			index := strings.Index(fileName, "_test_")
			if index == -1 {
				log.Fatalf("invalid test file %s", fileName)
			}
			currTestName = fileName[:index]
			currSQLTest = &scriptTest{testName: currTestName, testSuite: w, rnd: rand.New(rand.NewSource(time.Now().UTC().UnixNano()))}
			w.tests[currTestName] = currSQLTest
			currSQLTest.outFile = currTestName + "_test_out.txt"
		}
		if strings.HasSuffix(fileName, "_test_data.txt") {
			currSQLTest.testDataFile = fileName
		} else if strings.HasSuffix(fileName, "_test_script.txt") {
			currSQLTest.scriptFile = fileName
		}
	}
}

func (w *scriptTestSuite) stopCluster() {
	cfg := w.tektiteCluster[0].GetConfig()
	err := shutdown.PerformShutdown(&cfg, false)
	w.suite.Require().NoError(err)
	if w.objectStoreType == conf.DevObjectStoreType {
		if err := w.devObjStore.Stop(); err != nil {
			panic(err)
		}
	}
	log.Debug("**** stopped cluster")
}

func (w *scriptTestSuite) teardown() {
	w.stopCluster()
	if w.objectStoreType == conf.DevObjectStoreType {
		if err := w.devObjStore.Stop(); err != nil {
			panic(err)
		}
	}
}

type scriptTest struct {
	testSuite    *scriptTestSuite
	testName     string
	scriptFile   string
	testDataFile string
	outFile      string
	output       *strings.Builder
	rnd          *rand.Rand
	topics       []*fake.Topic
	cli          *cli.Cli
	clientNodeID int
	datasets     map[string]*dataset
}

func (st *scriptTest) run() {

	defer common.TektitePanicHandler()

	// Only run one test in the suite at a time
	st.testSuite.lock.Lock()
	defer st.testSuite.lock.Unlock()

	log.Infof("%s: Running sql test %s", st.testName, st.testName)

	req := st.testSuite.suite.Require()

	req.NotEmpty(st.scriptFile, fmt.Sprintf("sql test %s is missing script file %s", st.testName, fmt.Sprintf("%s_test_script.txt", st.testName)))
	req.NotEmpty(st.testDataFile, fmt.Sprintf("sql test %s is missing test data file %s", st.testName, fmt.Sprintf("%s_test_data.txt", st.testName)))
	req.NotEmpty(st.outFile, fmt.Sprintf("sql test %s is missing out file %s", st.testName, fmt.Sprintf("%s_test_out.txt", st.testName)))

	b, err := os.ReadFile("./testdata/" + st.scriptFile)
	req.NoError(err)
	scriptContents := string(b)

	err = st.loadDatasets()
	req.NoError(err)

	// All executable script commands whether they are special comments or sql statements must end with semi-colon followed by newline
	if scriptContents[len(scriptContents)-1] == ';' {
		scriptContents = scriptContents[:len(scriptContents)-1]
	}
	commands := strings.Split(scriptContents, ";\n")
	numIters := 1
	for iter := 0; iter < numIters; iter++ {
		numIts := st.runTestIteration(req, commands, iter)
		if iter == 0 {
			numIters = numIts
		}
	}
}

//nolint:gocyclo
func (st *scriptTest) runTestIteration(require *require.Assertions, commands []string, iter int) int {
	log.Infof("%s: Running test iteration %d", st.testName, iter)
	start := time.Now()
	st.output = &strings.Builder{}
	st.cli = st.createCli(require)
	numIters := 1
	for i, command := range commands {
		waitForInd := strings.Index(command, "wait for results")
		if waitForInd != -1 {
			st.output.WriteString(command[:waitForInd-1] + ";\n")
		} else {
			st.output.WriteString(command + ";\n")
		}
		command = trimBothEnds(command)
		if command == "" {
			continue
		}
		log.Infof("%s: Executing line: %s", st.testName, command)
		if strings.HasPrefix(command, "--load data") {
			st.executeLoadData(require, command)
		} else if strings.HasPrefix(command, "--produce data") {
			st.executeProduceData(require, command)
		} else if strings.HasPrefix(command, "--async produce data") {
			st.executeAsyncProduceData(require, command)
		} else if strings.HasPrefix(command, "--consume data") {
			st.executeConsumeData(require, command)
		} else if strings.HasPrefix(command, "--close client") {
			st.executeCloseClient(require)
		} else if strings.HasPrefix(command, "--repeat") {
			if i > 0 {
				require.Fail("--repeat command must be first line in script")
			}
			var err error
			n, err := strconv.ParseInt(command[9:], 10, 32)
			require.NoError(err)
			numIters = int(n)
		} else if strings.HasPrefix(command, "--create topic") {
			st.executeCreateTopic(require, command)
		} else if strings.HasPrefix(command, "--delete topic") {
			st.executeDeleteTopic(require, command)
		} else if strings.HasPrefix(command, "--restart cluster") {
			st.executeRestartCluster(require)
		} else if strings.HasPrefix(command, "--pause") {
			st.executePause(require, command)
		} else if strings.HasPrefix(command, "--breakpoint") {
			// Add a breakpoint in your debugger here, then add "--breakpoint" to your test script and it will stop at
			// that line, then you can add further breakpoints in your code
			log.Info("at breakpoint")
		}
		if strings.HasPrefix(command, "--") {
			// Just a normal comment - ignore
		} else {
			st.executeTektiteStatement(require, command)
		}
	}

	fmt.Println("TEST OUTPUT=========================\n" + st.output.String())
	fmt.Println("END TEST OUTPUT=====================")

	st.closeClient(require)

	for _, topic := range st.topics {
		err := st.testSuite.fakeKafka.DeleteTopic(topic.Name)
		require.NoError(err)
	}

	// compare the output with expected
	b, err := os.ReadFile("./testdata/" + st.outFile)
	require.NoError(err)
	expectedLines := strings.Split(string(b), "\n")
	actualLines := strings.Split(st.output.String(), "\n")
	ok := true
	if len(expectedLines) != len(actualLines) {
		ok = false
	} else {
		// We compare the lines one by one because there are special lines that we compare by prefix not exactly
		// E.g. internal error contains a UUID which is different each time so we can't exact compare
		for i, expected := range expectedLines {
			actual := actualLines[i]
			hasPrefix := false
			for _, prefix := range prefixCompareLines {
				if strings.HasPrefix(expected, prefix) {
					if !strings.HasPrefix(actual, prefix) {
						ok = false
					}
					hasPrefix = true
					break
				}
			}
			if !ok {
				break
			}
			a := strings.TrimRight(actual, " ")
			e := strings.TrimRight(expected, " ")
			if !hasPrefix && a != e {
				ok = false
				break
			}
		}
	}
	if !ok {
		require.Equal(string(b), st.output.String())
	}

	st.verifyRemainingData(require)

	dur := time.Since(start)
	log.Infof("%s: Finished running sql test %s time taken %d ms", st.testName, st.testName, dur.Milliseconds())
	return numIters
}

func (st *scriptTest) verifyRemainingData(require *require.Assertions) {
	// Now we verify that the test has left the cluster in a clean state
	start := time.Now()
	log.Infof("%s: verifying data at end of test", st.testName)
	for _, s := range st.testSuite.tektiteCluster {

		// undeploy of streams on other nodes is async, so we must wait
		_, err := testutils.WaitUntilWithError(func() (bool, error) {
			return s.GetStreamManager().StreamCount() == 0, nil
		}, 10*time.Second, 50*time.Millisecond)
		require.NoError(err)
		streamCount := s.GetStreamManager().StreamCount()
		if streamCount != 0 {
			log.Errorf("stream count for node %d is %d, now dumping streams", s.GetConfig().NodeID, streamCount)
			s.GetStreamManager().Dump()
			require.Fail("streams left at end of test")
		}
	}

	// We check there's no remaining user data that hasn't been deleted
	procCount := st.testSuite.tektiteCluster[0].GetConfig().ProcessorCount
	for procID := 0; procID < procCount; procID++ {
		leaderNode, err := st.testSuite.tektiteCluster[0].GetProcessorManager().GetLeaderNode(procID)
		require.NoError(err)
		processor := st.testSuite.tektiteCluster[leaderNode].GetProcessorManager().GetProcessor(procID)
		require.NotNil(processor)
		iter, err := processor.NewIterator(nil, nil, math.MaxUint64, false)
		require.NoError(err)
		for {
			valid, curr, err := iter.Next()
			require.NoError(err)
			if !valid {
				break
			}
			k := curr.Key
			slabID := int(binary.BigEndian.Uint64(k[16:]))
			if slabID >= common.UserSlabIDBase {
				// Find which processor and node owns this prefix
				partitionHash := k[:16]
				owningProcessorID := proc.CalcProcessorForHash(partitionHash, procCount)
				processorNode, err := st.testSuite.tektiteCluster[0].GetProcessorManager().GetLeaderNode(owningProcessorID)
				require.NoError(err)
				processor = st.testSuite.tektiteCluster[processorNode].GetProcessorManager().GetProcessor(owningProcessorID)
				require.NotNil(processor)
				// Then on this node there should be no data for this partition. The only reason we see data on
				// other nodes is because tombstone hasn't been pushed to level manager yet, but when executed on
				// the owning node, no data should be seen
				prefix := k[:24]
				rangeEnd := common.IncBigEndianBytes(prefix)
				// Now wait until valid as deleting streams is async
				ok, err := testutils.WaitUntilWithError(func() (bool, error) {
					iter2, err := processor.NewIterator(prefix, rangeEnd, math.MaxUint64, false)
					if err != nil {
						return false, err
					}
					defer iter2.Close()
					valid, curr, err := iter2.Next()
					if err != nil {
						return false, err
					}
					if valid {
						log.Errorf("found data for prefix %v slabID %d on node %d processor id %d - key %v value %v", prefix,
							slabID, processorNode, processor.ID(), curr.Key, curr.Value)
					}
					// We don't want to see any data
					return !valid, nil
				}, 10*time.Second, 25*time.Millisecond)
				require.True(ok)
				require.NoError(err)
			}
		}
		iter.Close()
	}
	log.Infof("verifying data at end of test took %d ms", time.Since(start).Milliseconds())
}

var prefixCompareLines = []string{"Internal error - reference:"}

type dataset struct {
	name        string
	encoderName string
	topicName   string
	data        [][]string
}

func (st *scriptTest) loadDatasets() error {
	dataFile, closeFunc := openFile("./testdata/" + st.testDataFile)
	defer closeFunc()
	scanner := bufio.NewScanner(dataFile)
	dataSets := map[string]*dataset{}
	var dataSet *dataset
	for scanner.Scan() {
		line := scanner.Text()
		line = trimBothEnds(line)
		if line == "" {
			break
		}
		if strings.HasPrefix(line, "dataset:") {
			line = line[8:]
			recordReader := csv.NewReader(strings.NewReader(line))
			record, err := recordReader.Read()
			if err != nil {
				return err
			}
			if dataSet != nil {
				dataSets[dataSet.name] = dataSet
			}
			dataSet = &dataset{}
			dataSet.name = record[0]
			dataSet.encoderName = record[1]
			dataSet.topicName = record[2]
		} else {
			recordReader := csv.NewReader(strings.NewReader(line))
			recordReader.Comma = ','
			record, err := recordReader.Read()
			if err != nil {
				return err
			}
			dataSet.data = append(dataSet.data, record)
		}
	}
	if dataSet != nil {
		dataSets[dataSet.name] = dataSet
		st.datasets = dataSets
	}
	return nil
}

func (st *scriptTest) executeLoadData(require *require.Assertions, command string) {
	datasetName := command[12:]
	dataSet, ok := st.datasets[datasetName]
	if !ok {
		require.Failf("cannot load data", "unknown dataset %s", datasetName)
	}
	encoder := &defaultEncoder{}
	var msgs []*kafka.Message
	for _, record := range dataSet.data {
		msg, err := encoder.EncodeMessage(record)
		require.NoError(err)
		msgs = append(msgs, msg)
	}
	topic, ok := st.testSuite.fakeKafka.GetTopic(dataSet.topicName)
	if !ok {
		require.Failf("cannot load data", "no such topic %s", dataSet.topicName)
	}

	initialIngestCount := st.getTotalIngestCount()
	for _, msg := range msgs {
		err := topic.Push(msg)
		require.NoError(err)
	}
	log.Debugf("waiting for %d msgs to be ingested, initial %d total %d", len(msgs),
		initialIngestCount, initialIngestCount+len(msgs))
	// First we must make sure the messages have been ingested
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		totIngest := st.getTotalIngestCount()
		return totIngest == initialIngestCount+len(msgs), nil
	}, 5*time.Second, 1*time.Millisecond)
	require.NoError(err)
	require.True(ok)

	maxVer := st.getMaxInProgressVersion()
	st.waitForVersion(require, maxVer, false)
}

func (st *scriptTest) executeProduceData(require *require.Assertions, command string) {
	datasetName := command[15:]
	dataSet, ok := st.datasets[datasetName]
	if !ok {
		require.Failf("cannot produce data", "unknown dataset %s", datasetName)
	}
	st.doExecuteProduceData(require, dataSet)
}

func (st *scriptTest) doExecuteProduceData(require *require.Assertions, dataSet *dataset) {

	encoder := &defaultEncoder{}
	var msgs []*kafka.Message
	for _, record := range dataSet.data {
		msg, err := encoder.EncodeMessage(record)
		require.NoError(err)
		msgs = append(msgs, msg)
	}

	kafkaEndpoint := st.testSuite.tektiteCluster[0].GetStreamManager().GetKafkaEndpoint(dataSet.topicName)
	if kafkaEndpoint == nil {
		require.Failf("cannot produce data", "unknown topic %s", dataSet.topicName)
	}
	st.produceMessages(require, msgs, dataSet.topicName)

	maxVer := st.getMaxInProgressVersion()
	st.waitForVersion(require, maxVer, false)
}

func (st *scriptTest) executeAsyncProduceData(require *require.Assertions, command string) {
	tokens := strings.Split(command[21:], " ")
	datasetName := tokens[0]
	dataSet, ok := st.datasets[datasetName]
	if !ok {
		require.Failf("cannot produce data", "unknown dataset %s", datasetName)
	}
	sDelay := tokens[1]
	delayMs, err := strconv.Atoi(sDelay)
	require.NoError(err)
	go func() {
		time.Sleep(time.Duration(delayMs) * time.Millisecond)
		st.doExecuteProduceData(require, dataSet)
	}()
}

func (st *scriptTest) getMaxInProgressVersion() int {
	// Get the highest current version to be broadcast to all query managers - this is the
	// version they use in queries
	maxVer := -1
	for _, s := range st.testSuite.tektiteCluster {
		ver := s.GetProcessorManager().GetCurrentVersion()
		if ver > maxVer {
			maxVer = ver
		}
	}
	// Increment by 1 as if proc manager curr version = X then version X + 1 could have be-in progress,
	// as barrier could be injected for X and data after that barrier would have version X + 1
	return maxVer + 1
}

func (st *scriptTest) waitForVersion(require *require.Assertions, ver int, flushed bool) {
	// Wait until query manager gets this ver
	ok, err := testutils.WaitUntilWithError(func() (bool, error) {
		for _, s := range st.testSuite.tektiteCluster {
			qm := s.GetQueryManager()
			var v int
			if flushed {
				v = qm.GetLastFlushedVersion()
			} else {
				v = qm.GetLastCompletedVersion()
			}
			if v < ver {
				return false, nil
			}
		}
		return true, nil
	}, 10*time.Second, 1*time.Millisecond)
	require.NoError(err)
	require.True(ok)
}

func (st *scriptTest) produceMessages(require *require.Assertions, msgs []*kafka.Message, topicName string) {
	s := st.chooseServer()
	producer, err := kafkago.NewProducer(&kafkago.ConfigMap{
		"partitioner":       "murmur2_random", // This matches the default hash algorithm we use, and same as Java client
		"bootstrap.servers": s.GetConfig().KafkaServerListenerConfig.Addresses[s.GetConfig().NodeID],
		"acks":              "all"})
	require.NoError(err)
	defer producer.Close()

	deliveryChan := make(chan kafkago.Event, len(msgs))
	for _, msg := range msgs {
		headers := make([]kafkago.Header, 0, len(msg.Headers))
		for _, hdr := range msg.Headers {
			headers = append(headers, kafkago.Header{
				Key:   hdr.Key,
				Value: hdr.Value,
			})
		}
		err := producer.Produce(&kafkago.Message{
			TopicPartition: kafkago.TopicPartition{Topic: &topicName, Partition: kafkago.PartitionAny},
			Key:            msg.Key,
			Value:          msg.Value,
			Headers:        headers,
			Timestamp:      msg.TimeStamp,
		},
			deliveryChan,
		)
		require.NoError(err)
		e := <-deliveryChan
		m := e.(*kafkago.Message)
		require.NoError(m.TopicPartition.Error)
	}
}

func (st *scriptTest) executeConsumeData(require *require.Assertions, command string) {
	tokens := strings.Split(command[15:], " ")
	consumerEndpointName := tokens[0]
	groupID := tokens[1]
	earliestOrLatest := tokens[2]
	sNumMessages := tokens[3]
	numMessages, err := strconv.Atoi(sNumMessages)
	sCommit := tokens[4]
	commit := sCommit == "commit"
	noPrintOffset := false
	if len(tokens) == 6 {
		sNoOffset := tokens[5]
		noPrintOffset = sNoOffset == "no_print_offset"
	}
	require.NoError(err)

	s := st.chooseServer()
	cm := &kafkago.ConfigMap{
		"bootstrap.servers":  s.GetConfig().KafkaServerListenerConfig.Addresses[s.GetConfig().NodeID],
		"group.id":           groupID,
		"auto.offset.reset":  earliestOrLatest,
		"enable.auto.commit": false,
		//	"debug":              "all",
	}
	consumer, err := kafkago.NewConsumer(cm)
	require.NoError(err)
	defer func() {
		err := consumer.Close()
		require.NoError(err)
	}()
	err = consumer.Subscribe(consumerEndpointName, nil)
	require.NoError(err)
	start := time.Now()
	if noPrintOffset {
		st.output.WriteString("key headers value timestamp\n")
	} else {
		st.output.WriteString("partition offset key headers value timestamp\n")
	}
	var msgs []*kafkago.Message
	partitionOffsets := map[int32]kafkago.TopicPartition{}
	for len(msgs) < numMessages {
		msg, err := consumer.ReadMessage(time.Second)
		if err != nil {
			if err.(kafkago.Error).Code() == kafkago.ErrTimedOut {
				require.True(time.Now().Sub(start) <= 10*time.Second, "timed out waiting to consume messages")
				continue
			}
			st.output.WriteString(err.Error() + "\n")
			return
		}
		require.NotNil(msg)
		msgs = append(msgs, msg)
		msg.TopicPartition.Offset++
		partitionOffsets[msg.TopicPartition.Partition] = msg.TopicPartition
	}

	// sort by key to make things deterministic
	sort.SliceStable(msgs, func(i, j int) bool {
		key1 := msgs[i].Key
		key2 := msgs[j].Key
		return bytes.Compare(key1, key2) < 0
	})

	for _, msg := range msgs {
		ts := msg.Timestamp.In(time.UTC)
		sTs := fmt.Sprintf("%d-%02d-%02d %02d:%02d:%02d.%06d",
			ts.Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), ts.Nanosecond()/1000)
		var line string
		if noPrintOffset {
			builder := strings.Builder{}
			for i, hdr := range msg.Headers {
				builder.WriteString(hdr.Key)
				builder.WriteRune(':')
				builder.WriteString(string(hdr.Value))
				if i != len(msg.Headers)-1 {
					builder.WriteRune(',')
				}
			}
			line = fmt.Sprintf("%s %s %s %s\n", string(msg.Key), builder.String(), string(msg.Value), sTs)
		} else {
			line = fmt.Sprintf("%d %d %s %s %s %s\n", msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), "",
				string(msg.Value), sTs)
		}
		st.output.WriteString(line)
	}

	if commit {
		var offsets []kafkago.TopicPartition
		for _, po := range partitionOffsets {
			offsets = append(offsets, po)
		}
		_, err := consumer.CommitOffsets(offsets)
		require.NoError(err)
	}
}

func (st *scriptTest) getTotalIngestCount() int {
	count := 0
	for _, s := range st.testSuite.tektiteCluster {
		count += s.GetStreamManager().GetIngestedMessageCount()
	}
	return count
}

func (st *scriptTest) executeCloseClient(require *require.Assertions) {
	// Closes then recreates the cli
	st.closeClient(require)
	st.cli = st.createCli(require)
}

func (st *scriptTest) closeClient(require *require.Assertions) {
	err := st.cli.Stop()
	require.NoError(err)
}

func (st *scriptTest) executeCreateTopic(require *require.Assertions, command string) {
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 3 || lp == 4, "Invalid create topic, should be --create topic topic_name [partitions]")
	topicName := parts[2]
	var partitions int64 = 20
	if len(parts) > 3 {
		var err error
		partitions, err = strconv.ParseInt(parts[3], 10, 64)
		require.NoError(err)
	}
	_, err := st.testSuite.fakeKafka.CreateTopic(topicName, int(partitions))
	require.NoError(err)
}

func (st *scriptTest) executeDeleteTopic(require *require.Assertions, command string) {
	parts := strings.Split(command, " ")
	lp := len(parts)
	require.True(lp == 3, "Invalid delete topic, should be --delete topic topic_name")
	topicName := parts[2]
	err := st.testSuite.fakeKafka.DeleteTopic(topicName)
	require.NoError(err)
}

func (st *scriptTest) executeRestartCluster(require *require.Assertions) {
	log.Debug("*** executing restart cluster")
	st.closeClient(require)
	cfg := st.testSuite.tektiteCluster[0].GetConfig()
	err := shutdown.PerformShutdown(&cfg, false)
	require.NoError(err)
	log.Debug("** shutdown cluster ok")
	confs := st.testSuite.createServerConfs()
	st.testSuite.startCluster(confs)
	log.Debug("** setup new cluster ok")
	st.cli = st.createCli(require)
	log.Debug("*** executed restart cluster")
}

func (st *scriptTest) executePause(require *require.Assertions, command string) {
	command = command[8:]
	pauseTime, err := strconv.ParseInt(command, 10, 32)
	log.Debugf("Pausing for %d ms", pauseTime)
	time.Sleep(time.Duration(pauseTime) * time.Millisecond)
	require.NoError(err)
}

func (st *scriptTest) executeTektiteStatement(require *require.Assertions, statement string) {
	start := time.Now()
	ind := strings.Index(statement, "wait for results")
	var waitUntilResults string
	if ind != -1 {
		waitUntilResults = statement[ind+17:] + "\n"
		statement = statement[:ind]
	}

	var res string
	if waitUntilResults == "" {
		res = st.execStatement(require, statement)
		log.Infof("%s output:%s", st.testName, res)
		st.output.WriteString(res)
	} else {
		ok, err := testutils.WaitUntilWithError(func() (bool, error) {
			res = st.execStatement(require, statement)
			return res == waitUntilResults, nil
		}, 5*time.Second, 10*time.Millisecond)
		require.NoError(err)
		res = st.execStatement(require, statement)
		if !ok {
			st.output.WriteString("DOES NOT MATCH EXPECTED\n")
		}
		st.output.WriteString(res)
	}
	end := time.Now()
	dur := end.Sub(start)
	log.Infof("%s: Statement execute time ms %d", st.testName, dur.Milliseconds())
}

func (st *scriptTest) execStatement(require *require.Assertions, statement string) string {
	resChan, err := st.cli.ExecuteStatement(statement)
	require.NoError(err)
	sb := strings.Builder{}
	for line := range resChan {
		sb.WriteString(line)
		sb.WriteRune('\n')
	}
	return sb.String()
}

func (st *scriptTest) chooseServer() *server.Server {
	servers := st.testSuite.tektiteCluster
	lp := len(servers)
	if lp == 1 {
		return servers[0]
	}
	// We choose a server randomly for executing statements - statements should work consistently irrespective
	// of what server they are run on
	index := st.rnd.Int31n(int32(lp))
	return servers[index]
}

func (st *scriptTest) createCli(require *require.Assertions) *cli.Cli {
	// We connect to a random Server
	s := st.chooseServer()
	id := s.GetConfig().NodeID
	address := s.GetConfig().HttpApiAddresses[id]
	tlsConf := client.TLSConfig{
		TrustedCertsPath: caSignedServerCertPath,
	}
	client := cli.NewCli(address, tlsConf)
	client.SetPageSize(clientPageSize)
	err := client.Start()
	require.NoError(err)
	st.clientNodeID = id
	return client
}

func trimBothEnds(str string) string {
	str = strings.TrimLeft(str, " \t\n")
	str = strings.TrimRight(str, " \t\n")
	return str
}

func openFile(fileName string) (*os.File, func()) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	closeFunc := func() {
		if err = file.Close(); err != nil {
			log.Fatal(err)
		}
	}
	return file, closeFunc
}

type defaultEncoder struct {
}

func (d *defaultEncoder) EncodeMessage(data []string) (*kafka.Message, error) {
	key := []byte(data[0])
	headerString := data[1]
	var headers []kafka.MessageHeader
	if headerString != "nil" {
		headerParts := strings.Split(headerString, ",")

		for _, headerPart := range headerParts {
			kvParts := strings.Split(headerPart, ":")
			if len(kvParts) != 2 {
				return nil, errwrap.Errorf("invalid header string %s in data", headerString)
			}
			headers = append(headers, kafka.MessageHeader{
				Key:   kvParts[0],
				Value: []byte(kvParts[1]),
			})
		}
	}
	value := []byte(data[2])
	tsVal := data[3]
	parsedTime, err := time.Parse("2006-01-02 15:04:05.000", tsVal)
	if err != nil {
		return nil, err
	}
	utcTime := parsedTime.UTC()
	msg := &kafka.Message{
		TimeStamp: utcTime,
		Key:       key,
		Value:     value,
		Headers:   headers,
	}
	return msg, nil
}

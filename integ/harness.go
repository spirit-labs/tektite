package integ

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/spirit-labs/tektite/logger"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
)

type Manager struct {
	lock   sync.Mutex
	idSeq  int
	agents map[int]*AgentProcess
}

func NewManager() *Manager {
	return &Manager{
		agents: map[int]*AgentProcess{},
	}
}

func (m *Manager) StartAgent(args string, captureOutput bool) (*AgentProcess, error) {
	m.lock.Lock()
	defer m.lock.Unlock()
	argsSlice := strings.Split(args, " ")
	agentProcess := &AgentProcess{
		mgr:           m,
		id:            m.idSeq,
		args:          argsSlice,
		captureOutput: captureOutput,
	}
	m.idSeq++
	if err := agentProcess.Start(); err != nil {
		return nil, err
	}
	m.agents[agentProcess.id] = agentProcess
	return agentProcess, nil
}

func (m *Manager) removeAgent(id int) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.agents, id)
}

type AgentProcess struct {
	mgr                  *Manager
	id                   int
	args                 []string
	cmd                  *exec.Cmd
	out                  io.ReadCloser
	captureOutput        bool
	outChan              chan []string
	allOut               []string
	startWG              sync.WaitGroup
	kafkaListenAddress   string
	clusterListenAddress string
}

func (m *Manager) RunAgentAndGetOutput(args string) ([]string, error) {
	argsSlice := strings.Split(args, " ")
	if len(argsSlice) == 1 && argsSlice[0] == "" {
		argsSlice = nil
	}
	cmd := exec.Command("../bin/tekagent", argsSlice...)
	// Set so help formats properly
	cmd.Env = append(os.Environ(), "COLUMNS=160")

	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	outString := out.String()
	fmt.Println(outString)

	scanner := bufio.NewScanner(&out)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func (a *AgentProcess) Start() error {
	cmd := exec.Command("../bin/tekagent", a.args...)
	cmd.Env = append(os.Environ(), "COLUMNS=160")
	out, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	cmd.Stderr = cmd.Stdout
	a.startWG.Add(1)
	if err := cmd.Start(); err != nil {
		return err
	}
	a.cmd = cmd
	a.out = out
	a.outChan = make(chan []string, 1)
	go func() {
		if err := a.outputLoop(a.outChan); err != nil {
			log.Errorf("failure in capturing agent output: %v", err)
			a.outChan <- []string{err.Error()}
		}
	}()
	// We wait until the agent has started running , so the signal handler has been set otherwise if we stop it quickly
	// the signal won't be intercepted
	a.startWG.Wait()
	return nil
}

func (a *AgentProcess) Stop() error {
	// Send a SIGINT signal
	if err := a.cmd.Process.Signal(syscall.SIGINT); err != nil {
		return err
	}
	// Wait for process to exit
	if err := a.cmd.Wait(); err != nil {
		return err
	}
	a.allOut = <-a.outChan
	a.mgr.removeAgent(a.id)
	return nil
}

func (a *AgentProcess) Output() []string {
	return a.allOut
}

func (a *AgentProcess) outputLoop(outChan chan []string) error {
	scanner := bufio.NewScanner(a.out)
	var allOut []string
	for scanner.Scan() {
		line := scanner.Text()
		prefix := "started tektite agent with kafka listener:"
		if strings.HasPrefix(line, prefix) {
			second := " and internal listener:"
			index := strings.LastIndex(line, second)
			if index == -1 {
				return errors.Errorf("unexpected agent startup line: %s", line)
			}
			// Extract the actual listen addresses as they could be listening specifying ephemeral port (0)
			a.kafkaListenAddress = line[len(prefix):index]
			a.clusterListenAddress = line[index+len(second):]
			a.startWG.Done()
		}
		fmt.Println(fmt.Sprintf("agent: %d: %s", a.id, line))
		if a.captureOutput {
			allOut = append(allOut, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	outChan <- allOut
	return nil
}

func init() {
	if err := checkBinary(); err != nil {
		log.Errorf("failed to build agent binary: %v", err)
	}
}

// We must make sure the agent binary is up-to-date before running tests
func checkBinary() error {
	_, err := os.Stat("../bin/tekagent")
	if os.IsNotExist(err) {
		log.Infof("building agent binary")
		cmd := exec.Command("go", "build", "-o", "../bin", "../agent/tekagent")
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		if err := cmd.Run(); err != nil {
			log.Errorf("failed to build agent binary:\n%s", out.String())
			return err
		}
	}
	return nil
}

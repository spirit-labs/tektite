package testutils

import (
	"bufio"
	"fmt"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
)

var (
	_, b, _, _          = runtime.Caller(0)
	thisDir             = filepath.Dir(b)
	portServiceLockPath = filepath.Join(thisDir, "tektite_port_service.lock")
	portsFilePath       = filepath.Join(thisDir, "tektite_port_service_ports.txt")
)

var PortProvider = NewPortService(32323)

const numPortsPerTest = 25

type PortSequence struct {
	seq     int
	lastSeq int
}

func (ps *PortSequence) NextPort() int {
	if ps.seq == ps.lastSeq {
		panic("not enough ports in sequence")
	}
	next := ps.seq
	ps.seq++
	return next
}

func NewPortService(portStart int) *PortService {
	return &PortService{
		portStart: portStart,
		sequences: map[string]*PortSequence{},
	}
}

type PortService struct {
	lock      sync.Mutex
	loaded    bool
	entries   []PortRangeEntry
	portStart int
	sequences map[string]*PortSequence
}

type PortRangeEntry struct {
	testName  string
	portStart int
	portEnd   int
}

func (p *PortService) ResetSequence(t *testing.T) {
	p.lock.Lock()
	defer p.lock.Unlock()
	delete(p.sequences, t.Name())
}

func (p *PortService) GetPort(t *testing.T) int {
	p.lock.Lock()
	defer p.lock.Unlock()
	testName := t.Name()
	seq, ok := p.sequences[testName]
	if ok {
		return seq.NextPort()
	}
	seq, err := p.createSequence(testName, numPortsPerTest)
	if err != nil {
		panic(fmt.Sprintf("post service failed to create sequence: %v", err))
	}
	p.sequences[testName] = seq
	return seq.NextPort()
}

func (p *PortService) createSequence(testName string, numPorts int) (*PortSequence, error) {
	if p.loaded {
		seq := p.findEntry(testName)
		if seq != nil {
			return seq, nil
		}
	}

	// get exclusive file lock
	fl := newFlock(portServiceLockPath)
	fl.lock()

	content, err := os.ReadFile(portsFilePath)
	if err == nil {
		p.entries = nil

		// load existing ports file
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			parts := strings.Split(line, ",")
			if len(parts) != 3 {
				return nil, errors.Errorf("invalid ports entry: %s", line)
			}
			testName := parts[0]
			portStart, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, err
			}
			portEnd, err := strconv.Atoi(parts[2])
			if err != nil {
				return nil, err
			}
			p.entries = append(p.entries, PortRangeEntry{
				testName:  testName,
				portStart: portStart,
				portEnd:   portEnd,
			})
		}
	} else {
		if os.IsNotExist(err) {
			// OK
		} else {
			return nil, err
		}
	}

	seq := p.findEntry(testName)
	if seq == nil {

		nextFreePort := p.nextFreePort()

		// entry not found, add new entry then delete existing ports file (if any) and save new one
		entry := PortRangeEntry{
			testName:  testName,
			portStart: nextFreePort,
			portEnd:   nextFreePort + numPorts,
		}
		p.entries = append(p.entries, entry)
		seq = &PortSequence{
			seq:     entry.portStart,
			lastSeq: entry.portEnd,
		}

		// replace locks file

		portsFile, err := os.Create(portsFilePath)
		if err != nil {
			return nil, err
		}
		writer := bufio.NewWriter(portsFile)
		for i, entry := range p.entries {
			// we don't add a newline on the last line
			newLine := ""
			if i != len(p.entries)-1 {
				newLine = "\n"
			}
			_, err := writer.WriteString(fmt.Sprintf("%s,%d,%d%s", entry.testName, entry.portStart, entry.portEnd, newLine))
			if err != nil {
				return nil, err
			}
		}
		if err := writer.Flush(); err != nil {
			return nil, err
		}
		if err := portsFile.Close(); err != nil {
			return nil, err
		}
	}

	// release the lock
	log.Debugf("port service %p releasing file lock", p)
	fl.unlock()
	p.loaded = true
	return seq, nil
}

func (p *PortService) findEntry(testName string) *PortSequence {
	for _, entry := range p.entries {
		if entry.testName == testName {
			return &PortSequence{
				seq:     entry.portStart,
				lastSeq: entry.portEnd,
			}
		}
	}
	return nil
}

func (p *PortService) nextFreePort() int {
	if len(p.entries) == 0 {
		return p.portStart
	}
	return p.entries[len(p.entries)-1].portEnd
}

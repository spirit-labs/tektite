//go:build !main

package testutils

import (
	"context"
	"fmt"
	"github.com/alexflint/go-filemutex"
	log "github.com/spirit-labs/tektite/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	etcdLockPath         = filepath.Join(thisDir, "tektite_etcd_service.lock")
	etcdRefCountFilePath = filepath.Join(thisDir, "tektite_etcd_refcount.txt")
	etcdPidFilePath      = filepath.Join(thisDir, "tektite_etcd_pid.txt")
)

const etcdCommand = "etcd"

var etcd etcdInfo

type etcdInfo struct {
	lock sync.Mutex
	pid  int
}

func newFlock(fileName string) *flock {
	fm, err := filemutex.New(fileName)
	if err != nil {
		panic(fmt.Sprintf("failed to create filelock: %v", err))
	}
	return &flock{fm: fm}
}

type flock struct {
	fm *filemutex.FileMutex
}

func (f *flock) lock() {
	if err := f.fm.Lock(); err != nil {
		panic(fmt.Sprintf("failed to lock filelock: %v", err))
	}
}

func (f *flock) unlock() {
	if err := f.fm.Unlock(); err != nil {
		panic(fmt.Sprintf("failed to unlock filelock: %v", err))
	}
}

func (e *etcdInfo) require() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	fl := newFlock(etcdLockPath)
	fl.lock()
	defer fl.unlock()

	count, err := e.readRefCount()
	if err != nil {
		cleanupAndExit()
	}

	pid := -1
	content, err := os.ReadFile(etcdPidFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		pid, err = strconv.Atoi(string(content))
		if err != nil {
			return err
		}
		if !isProcessRunning(pid) {
			cleanupAndExit()
		}
	}
	if count == 0 {
		// check if etcd already running
		cmd := exec.Command("ps", "aux")
		output, err := cmd.Output()
		if err != nil {
			return err
		}
		if strings.Contains(string(output), "etcd") {
			panic("etcd process already running. please kill it and re-run tests")
		}

		pid = startEtcd()
		spid := strconv.Itoa(pid)
		if err := os.WriteFile(etcdPidFilePath, []byte(spid), 0644); err != nil {
			return err
		}
	} else {
		if pid == -1 {
			cleanupAndExit()
		}
	}
	e.pid = pid
	count++
	if err := e.writeRefCount(count); err != nil {
		return err
	}
	return nil
}

func cleanupAndExit() {
	if err := os.Remove(etcdPidFilePath); err != nil {
		// Ignore
	}
	if err := os.Remove(etcdRefCountFilePath); err != nil {
		// Ignore
	}
	panic("previous test run left state in directory. please re-run tests")
}

func isProcessRunning(pid int) bool {
	if _, err := syscall.Getpgid(pid); err != nil {
		return false
	}
	return true
}

func (e *etcdInfo) release() error {
	e.lock.Lock()
	defer e.lock.Unlock()

	fl := newFlock(etcdLockPath)
	fl.lock()
	defer fl.unlock()

	count, err := e.readRefCount()
	if err != nil {
		return err
	}

	count--
	if count < 0 {
		panic("etcd refCount < 0")
	}

	if count == 0 {
		stopEtcd(e.pid)
		if err := os.Remove(etcdPidFilePath); err != nil {
			return err
		}
	}
	if err := e.writeRefCount(count); err != nil {
		return err
	}
	return nil
}

func (e *etcdInfo) readRefCount() (int, error) {
	content, err := os.ReadFile(etcdRefCountFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	count, err := strconv.Atoi(string(content))
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (e *etcdInfo) writeRefCount(count int) error {
	sCount := strconv.Itoa(count)
	return os.WriteFile(etcdRefCountFilePath, []byte(sCount), 0644)
}

func startEtcd() int {
	deleteEtcDir()
	start := time.Now()
	cmd := exec.Command(etcdCommand)
	err := cmd.Start()
	if err != nil {
		panic(fmt.Sprintf("etcdx:failed to start etcd %v", err))
	}
	for !tryExecuteOp() {
		// Wait for it to startup and be operational
		time.Sleep(10 * time.Millisecond)
	}
	log.Debugf("etcdx: etcd started in %d ms", time.Since(start).Milliseconds())
	return cmd.Process.Pid
}

func stopEtcd(pid int) {
	cmd := exec.Command("kill", "-TERM", strconv.Itoa(pid))

	// Set the process group ID to ensure that all child processes are also terminated
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Run()
	if err != nil {
		panic(fmt.Sprintf("failed to kill etcd process: %v", err))
	}
	deleteEtcDir()
	log.Debug("etcd stopped")
}

func RequireEtcd() {
	if err := etcd.require(); err != nil {
		panic(fmt.Sprintf("failed to require etcd: %v", err))
	}
}

func ReleaseEtcd() {
	if err := etcd.release(); err != nil {
		panic(fmt.Sprintf("failed to release etcd: %v", err))
	}
}

func tryExecuteOp() bool {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 100 * time.Millisecond,
	})
	//goland:noinspection GoUnhandledErrorResult
	defer cli.Close()
	if err != nil && checkUnavailableError(err) {
		return false
	}

	resp, err := cli.Get(context.Background(), "__tektitetest_ready_key")
	if err != nil && checkUnavailableError(err) {
		return false
	}

	if len(resp.Kvs) > 0 {
		log.Errorf("etcdx: __tektitetest_ready_key key already exists:%d - NOTE go test must be run with -p 1")
		panic("etcdx: __tektitetest_ready_key key already exists")
		return true
	}

	_, err = cli.Put(context.Background(), "__tektitetest_ready_key", "")
	if err != nil && checkUnavailableError(err) {
		return false
	}
	log.Debugf("etcdx: successfully put __tektitetest_ready_key key")
	return true
}

func checkUnavailableError(err error) bool {
	if e, ok := status.FromError(err); ok {
		if e.Code() == codes.Unavailable || e.Code() == codes.Canceled || e.Code() == codes.DeadlineExceeded {
			return true
		}
	}
	panic(err)
}

func deleteEtcDir() {
	if err := os.RemoveAll("default.etcd"); err != nil {
		log.Debug("etcdx:failed to delete etcd dir")
	} else {
		log.Debug("etcdx:successfully deleted etcd dir")
	}
}

package common

import (
	"crypto/tls"
	"fmt"
	"github.com/spirit-labs/tektite/errors"
	log "github.com/spirit-labs/tektite/logger"
	"math"
	"os"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"
	"unsafe"
)

func ByteSliceToStringZeroCopy(bs []byte) string {
	lbs := len(bs)
	if lbs == 0 {
		return ""
	}
	return unsafe.String(&bs[0], lbs)
}

func StringToByteSliceZeroCopy(str string) []byte {
	if str == "" {
		return nil
	}
	return unsafe.Slice(unsafe.StringData(str), len(str))
}

// DumpStacks dumps stacks for all goroutines to stdout, useful when debugging
//
//goland:noinspection GoUnusedExportedFunction
func DumpStacks() {
	buf := make([]byte, 1<<16)
	l := runtime.Stack(buf, true)
	s := string(buf[:l])
	fmt.Println(s)
}

func GetCurrentStack() string {
	buf := make([]byte, 1<<16)
	l := runtime.Stack(buf, false)
	return string(buf[:l])
}

// IncrementBytesBigEndian returns a new byte slice which is 1 larger than the provided slice when represented in
// big endian layout, but without changing the key length
func IncrementBytesBigEndian(bytes []byte) []byte {
	if len(bytes) == 0 {
		panic("cannot increment empty []byte")
	}
	inced := CopyByteSlice(bytes)
	lb := len(bytes)
	for i := lb - 1; i >= 0; i-- {
		b := bytes[i]
		if b < 255 {
			inced[i] = b + 1
			break
		}
		if i == 0 {
			panic(fmt.Sprintf("cannot increment key - all bits set: %v", bytes))
		}
		inced[i] = 0
	}
	return inced
}

func CopyByteSlice(buff []byte) []byte {
	res := make([]byte, len(buff))
	copy(res, buff)
	return res
}

const atFalse = 0
const atTrue = 1

type AtomicBool struct {
	val int32
}

func (a *AtomicBool) Get() bool {
	i := atomic.LoadInt32(&a.val)
	return i == atTrue
}

func (a *AtomicBool) Set(val bool) {
	atomic.StoreInt32(&a.val, a.toInt(val))
}

func (a *AtomicBool) toInt(val bool) int32 {
	// Uggghhh, why doesn't golang have an immediate if construct?
	var i int32
	if val {
		i = atTrue
	} else {
		i = atFalse
	}
	return i
}

func (a *AtomicBool) CompareAndSet(expected bool, val bool) bool {
	return atomic.CompareAndSwapInt32(&a.val, a.toInt(expected), a.toInt(val))
}

func GetOrDefaultIntProperty(propName string, props map[string]string, def int) (int, error) {
	ncs, ok := props[propName]
	var res int
	if ok {
		nc, err := strconv.ParseInt(ncs, 10, 32)
		if err != nil {
			return 0, err
		}
		res = int(nc)
	} else {
		res = def
	}
	return res, nil
}

func CreateKeyPair(certPath string, keyPath string) (tls.Certificate, error) {
	clientCert, err := os.ReadFile(certPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	clientKey, err := os.ReadFile(keyPath)
	if err != nil {
		return tls.Certificate{}, err
	}
	keyPair, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		return tls.Certificate{}, err
	}
	return keyPair, nil
}

func CallWithRetryOnUnavailable[R any](action func() (R, error), stopped func() bool) (R, error) {
	return CallWithRetryOnUnavailableWithTimeout(action, stopped, 10*time.Millisecond, time.Duration(math.MaxInt64), "failed to execute retryable operation")
}

func CallWithRetryOnUnavailableWithTimeout[R any](action func() (R, error), stopped func() bool, delay time.Duration,
	timeout time.Duration, retryMessage string) (R, error) {
	start := NanoTime()
	var zeroResult R
	for !stopped() {
		r, err := action()
		if err != nil {
			var perr errors.TektiteError
			if errors.As(err, &perr) {
				if perr.Code == errors.Unavailable {
					if retryMessage != "" {
						log.Warnf(fmt.Sprintf("%s: %v", retryMessage, err))
					}
					time.Sleep(delay)
					if NanoTime()-start >= uint64(timeout) {
						return zeroResult, errors.New("timed out waiting to execute action")
					}
					continue
				}
			}
			return zeroResult, err
		}
		return r, nil
	}
	return zeroResult, errors.New("retry terminated as closed")
}

func IsTektiteErrorWithCode(err error, code errors.ErrorCode) bool {
	var perr errors.TektiteError
	if errors.As(err, &perr) {
		if perr.Code == code {
			return true
		}
	}
	return false
}

func IsUnavailableError(err error) bool {
	return IsTektiteErrorWithCode(err, errors.Unavailable)
}

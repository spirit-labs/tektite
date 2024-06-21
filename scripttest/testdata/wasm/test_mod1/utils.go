// Copyright 2024 The Tektite Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// #include <stdlib.h>
import "C"

import (
	"runtime"
	"unsafe"
)

func ptrToString(ptr uint64) string {
	p := uint32(ptr >> 32)
	s := uint32(ptr)
	return unsafe.String((*byte)(unsafe.Pointer(uintptr(p))), s)
}

func ptrToBytes(ptr uint64) []byte {
	p := uint32(ptr >> 32)
	s := uint32(ptr)
	return unsafe.Slice((*byte)(unsafe.Pointer(uintptr(p))), s)
}

func stringToPtr(s string) (uint32, uint32) {
	ptr := unsafe.Pointer(unsafe.StringData(s))
	return uint32(uintptr(ptr)), uint32(len(s))
}

func stringToReturnedPtr(s string) uint64 {
	size := C.ulong(len(s))
	ptr := unsafe.Pointer(C.malloc(size))
	copy(unsafe.Slice((*byte)(ptr), size), s)
	return (uint64(uintptr(ptr)) << 32) | uint64(size)
}

func bytesToReturnedPtr(b []byte) uint64 {
	size := C.ulong(len(b))
	ptr := unsafe.Pointer(C.malloc(size))
	copy(unsafe.Slice((*byte)(ptr), size), b)
	return (uint64(uintptr(ptr)) << 32) | uint64(size)
}

func log(message string) {
	ptr, size := stringToPtr(message)
	_log(ptr, size)
	runtime.KeepAlive(message) // keep message alive until ptr is no longer needed.
}

//go:wasmimport env log
func _log(ptr, size uint32)

package main

// required for TinyGo to compile to Wasm
func main() {}

//export quux
func quux(intVal int64) int64 {
	return intVal + 1
}

//export kweep
func kweep(intVal int64) int64 {
	return intVal + 2
}

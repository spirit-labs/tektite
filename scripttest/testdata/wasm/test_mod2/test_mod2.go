package main

// required for TinyGo to compile to Wasm
func main() {}

//export filterFunc
func filterFunc(intVal int64) bool {
	return intVal%2 == 0
}

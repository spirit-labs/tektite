package main

// required for TinyGo to compile to Wasm
func main() {}

//export foo
func foo(intVal int64) int64 {
	return intVal + 1
}

//export bar
func bar(intVal int64) int64 {
	return intVal + 2
}

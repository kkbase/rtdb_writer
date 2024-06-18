package main

// #cgo CFLAGS: -I.
// #cgo LDFLAGS: -L. -lcwrite_plugin
// #include "write_plugin.h"
import "C"
import "unsafe"

//export Login
func Login() {
	C.login()
}

//export Logout
func Logout() {
	C.logout()
}

//export WriteAnalog
func WriteAnalog(time C.int64_t, analogArrayPtr unsafe.Pointer, count C.int64_t) {
	C.write_analog(time, (*C.Analog)(analogArrayPtr), count)
}

//export WriteDigital
func WriteDigital(time C.int64_t, digitalArrayPtr unsafe.Pointer, count C.int64_t) {
	C.write_digital(time, (*C.Digital)(digitalArrayPtr), count)
}

//export WriteStaticAnalog
func WriteStaticAnalog(staticAnalogArrayPtr unsafe.Pointer, count C.int64_t) {
	C.write_static_analog((*C.StaticAnalog)(staticAnalogArrayPtr), count)
}

//export WriteStaticDigital
func WriteStaticDigital(staticDigitalArrayPtr unsafe.Pointer, count C.int64_t) {
	C.write_static_digital((*C.StaticDigital)(staticDigitalArrayPtr), count)
}

func main() {}

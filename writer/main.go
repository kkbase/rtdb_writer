package main

// #cgo CFLAGS: -I../plugin
// #include "write_plugin.h"
import "C"
import (
	"fmt"
	"sync"
)

type AnalogSection struct {
	Time int64
	Data []C.Analog
}

type DigitalSection struct {
	Time int64
	Data []C.Digital
}

type StaticAnalogSection struct {
	Data []C.StaticAnalog
}

type StaticDigitalSection struct {
	Data []C.StaticDigital
}

// ReadAnalogCsv 读取CSV文件, 将其转换成 C.Analog 结构后发送到缓存队列
func ReadAnalogCsv(filename string, ch chan []AnalogSection, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	close(ch)
}

// ReadDigitalCsv 读取CSV文件, 将其转换成 C.Digital 结构后发送到缓存队列
func ReadDigitalCsv(filename string, ch chan []DigitalSection, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	close(ch)
}

// ReadStaticAnalogCsv 读取CSV文件, 将其转换成 []C.StaticAnalog 切片
func ReadStaticAnalogCsv(filename string) StaticAnalogSection {
	return StaticAnalogSection{}
}

// ReadStaticDigitalCsv 读取CSV文件, 将其转换成 []C.StaticDigital 切片
func ReadStaticDigitalCsv(filename string) StaticDigitalSection {
	return StaticDigitalSection{}
}

// FastWriteWriteRealtimeSection 极速写入实时断面
func FastWriteWriteRealtimeSection(fastAnalogCh chan []AnalogSection, fastDigitalCh chan []DigitalSection, normalAnalogCh chan []AnalogSection, normalDigitalCh chan []DigitalSection, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()
	closeChNum := 0
	for {
		select {
		case _, ok := <-fastAnalogCh:
			if !ok {
				closeChNum += 1
			} else {
				fmt.Println("cgo, write")
			}
		case _, ok := <-fastDigitalCh:
			if !ok {
				closeChNum += 1
			} else {
				fmt.Println("cgo, write")
			}
		case _, ok := <-normalAnalogCh:
			if !ok {
				closeChNum += 1
			} else {
				fmt.Println("cgo, write")
			}
		case _, ok := <-normalDigitalCh:
			if !ok {
				closeChNum += 1
			} else {
				fmt.Println("cgo, write")
			}
		}

		if closeChNum == 4 {
			break
		}
	}
}

func WriteHistorySection() {
}

func FastWrite(fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string) {
	fastAnalogCh := make(chan []AnalogSection)
	fastDigitalCh := make(chan []DigitalSection)
	normalAnalogCh := make(chan []AnalogSection)
	normalDigitalCh := make(chan []DigitalSection)
	wg := new(sync.WaitGroup)

	go ReadAnalogCsv(fastAnalogCsvPath, fastAnalogCh, wg)
	go ReadDigitalCsv(fastDigitalCsvPath, fastDigitalCh, wg)
	go ReadAnalogCsv(normalAnalogCsvPath, normalAnalogCh, wg)
	go ReadDigitalCsv(normalDigitalCsvPath, normalDigitalCh, wg)

	FastWriteWriteRealtimeSection(fastAnalogCh, fastDigitalCh, normalAnalogCh, normalDigitalCh, wg)

	wg.Wait()
}

func main() {
	fastAnalogCsvPath := "../CSV20240614/1718350759143_REALTIME_FAST_ANALOG.csv"
	fastDigitalCsvPath := "../CSV20240614/1718350759143_REALTIME_FAST_DIGITAL.csv"
	normalAnalogCsvPath := "../CSV20240614/1718350759143_REALTIME_NORMAL_ANALOG.csv"
	normalDigitalCsvPath := "../CSV20240614/1718350759143_REALTIME_NORMAL_DIGITAL.csv"
	FastWrite(fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath)
}

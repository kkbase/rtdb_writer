package main

// #cgo CFLAGS: -I../plugin
// #include "write_plugin.h"
import "C"
import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
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
func ReadAnalogCsv(filepath string, ch chan []AnalogSection, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

	// CSV读取器
	reader := csv.NewReader(bufio.NewReader(file))

	// 按行读取
	for {
		// 读取一行, 判断是否为EOF
		_, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			} else {
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		// 解析行

		// 发送数据
		// ch <- record
	}

	close(ch)
}

// ReadDigitalCsv 读取CSV文件, 将其转换成 C.Digital 结构后发送到缓存队列
func ReadDigitalCsv(filepath string, ch chan []DigitalSection, wg *sync.WaitGroup) {
	wg.Add(1)
	defer wg.Done()

	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

	// CSV读取器
	reader := csv.NewReader(bufio.NewReader(file))

	// 按行读取
	for {
		// 读取一行, 判断是否为EOF
		_, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			} else {
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		// 解析行

		// 发送数据
		// ch <- record
	}

	close(ch)
}

// ReadStaticAnalogCsv 读取CSV文件, 将其转换成 []C.StaticAnalog 切片
func ReadStaticAnalogCsv(filepath string) StaticAnalogSection {
	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

	return StaticAnalogSection{}
}

// ReadStaticDigitalCsv 读取CSV文件, 将其转换成 []C.StaticDigital 切片
func ReadStaticDigitalCsv(filepath string) StaticDigitalSection {
	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

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
	wdDir, err := os.Getwd()
	if err != nil {
		panic("get word dir err")
	}
	fastAnalogCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_FAST_ANALOG.csv"
	fastDigitalCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_FAST_DIGITAL.csv"
	normalAnalogCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_NORMAL_ANALOG.csv"
	normalDigitalCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_NORMAL_DIGITAL.csv"
	FastWrite(fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath)
}

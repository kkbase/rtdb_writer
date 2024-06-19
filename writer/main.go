package main

import "C"
import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// #cgo CFLAGS: -I../plugin
// #include "dylib.h"
// #include "write_plugin.h"
import "C"

const CacheSize = 64

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
func ReadAnalogCsv(filepath string, ch chan AnalogSection, wg *sync.WaitGroup, closeCh chan bool) {
	defer wg.Done()
	fmt.Println("filepath: ", filepath)
	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

	// CSV读取器
	reader := csv.NewReader(bufio.NewReader(file))

	dataList := make([]C.Analog, 0)
	timeFlag := int64(-1)
	// 按行读取
	for {
		// 读取一行, 判断是否为EOF
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				if len(dataList) != 0 {
					fmt.Println("send!: ", len(dataList), timeFlag)
					ch <- AnalogSection{Time: timeFlag, Data: dataList}
				}
				closeCh <- true
				break
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		// 去除首行
		if record[0] == "TIME" {
			continue
		}
		// 去除尾行
		if len(record) != 11 {
			continue
		}

		// 解析行
		// TIME,P_NUM,AV,AVR,Q,BF,FQ,FAI,MS,TEW,CST
		time, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			log.Println("parse time error", record[0])
			continue
		}
		pNum, err := strconv.ParseInt(record[1], 10, 32)
		if err != nil {
			log.Println("parse pNum error", record[1])
			continue
		}
		av, err := strconv.ParseFloat(record[2], 64)
		if err != nil {
			log.Println("parse av error", record[2])
			continue
		}
		avr, err := strconv.ParseFloat(record[3], 64)
		if err != nil {
			log.Println("parse avr error", record[3])
			continue
		}
		q, err := strconv.ParseBool(record[4])
		if err != nil {
			log.Println("parse q error", record[4])
			continue
		}
		bf, err := strconv.ParseBool(record[5])
		if err != nil {
			log.Println("parse bf error", record[5])
			continue
		}
		qf, err := strconv.ParseBool(record[6])
		if err != nil {
			log.Println("parse qf error", record[6])
			continue
		}
		fai, err := strconv.ParseFloat(record[7], 32)
		if err != nil {
			log.Println("parse fai error", record[7])
			continue
		}
		ms, err := strconv.ParseBool(record[8])
		if err != nil {
			log.Println("parse ms error", record[8])
			continue
		}
		if len(record[9]) != 1 {
			log.Println("parse tew error", record[9])
			continue
		}
		tew := record[9][0]

		cst, err := strconv.ParseInt(strings.TrimSuffix(record[10], "\r"), 10, 32)
		if err != nil {
			log.Println("parse cst error", record[10])
			continue
		}

		// time 初始化
		if timeFlag == -1 {
			timeFlag = time
		}

		if len(dataList)%1000 != 0 {
			fmt.Println("len: ", len(dataList))
		}

		// 如果出现的时间戳, 则更新timeFlag, 发送数据, 并且清空dataList
		if timeFlag != time {
			if len(dataList) != 0 {
				fmt.Println("send!: ", len(dataList), timeFlag)
				ch <- AnalogSection{Time: timeFlag, Data: dataList}
			}
			timeFlag = time

			dataList = make([]C.Analog, 0)
		}

		// 拼接数据, 并且添加到dataList
		analog := C.Analog{}
		analog.p_num = C.int32_t(pNum)
		analog.av = C.float(av)
		analog.avr = C.float(avr)
		analog.q = C.bool(q)
		analog.bf = C.bool(bf)
		analog.qf = C.bool(qf)
		analog.fai = C.float(fai)
		analog.ms = C.bool(ms)
		analog.tew = C.char(tew)
		analog.cst = C.uint16_t(cst)

		dataList = append(dataList, analog)
	}

	close(ch)
}

// ReadDigitalCsv 读取CSV文件, 将其转换成 C.Digital 结构后发送到缓存队列
func ReadDigitalCsv(filepath string, ch chan DigitalSection, wg *sync.WaitGroup, closeCh chan bool) {
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
	dataList := make([]C.Digital, 0)
	timeFlag := int64(-1)
	for {
		// 读取一行, 判断是否为EOF
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				if len(dataList) != 0 {
					ch <- DigitalSection{Time: timeFlag, Data: dataList}
				}
				closeCh <- true
				break
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		// 去除首行
		if record[0] == "TIME" {
			continue
		}
		// 去除尾行
		if len(record) != 11 {
			continue
		}

		time, err := strconv.ParseInt(record[0], 10, 64)
		if err != nil {
			log.Println("parse time error", record[0])
			continue
		}
		pNum, err := strconv.ParseInt(record[1], 10, 32)
		if err != nil {
			log.Println("parse pNum error", record[1])
			continue
		}
		dv, err := strconv.ParseBool(record[2])
		if err != nil {
			log.Println("parse dv error", record[2])
			continue
		}
		dvr, err := strconv.ParseBool(record[3])
		if err != nil {
			log.Println("parse dvr error", record[3])
			continue
		}
		q, err := strconv.ParseBool(record[4])
		if err != nil {
			log.Println("parse q error", record[4])
			continue
		}
		bf, err := strconv.ParseBool(record[5])
		if err != nil {
			log.Println("parse bf error", record[5])
			continue
		}
		bq, err := strconv.ParseBool(record[6])
		if err != nil {
			log.Println("parse bq error", record[6])
			continue
		}
		fai, err := strconv.ParseBool(record[7])
		if err != nil {
			log.Println("parse fai error", record[7])
			continue
		}
		ms, err := strconv.ParseBool(record[8])
		if err != nil {
			log.Println("parse ms error", record[8])
			continue
		}
		if len(record[9]) != 1 {
			log.Println("parse tew error", record[9])
			continue
		}
		tew := record[9][0]
		cst, err := strconv.ParseInt(strings.TrimSuffix(record[10], "\r"), 10, 32)
		if err != nil {
			log.Println("parse cst error", record[10])
			continue
		}

		// time 初始化
		if timeFlag == -1 {
			timeFlag = time
		}

		if len(dataList)%1000 == 0 {
			fmt.Println("len: ", len(dataList))
		}

		// 如果出现的时间戳, 则更新timeFlag, 发送数据, 并且清空dataList
		if timeFlag != time {
			if len(dataList) != 0 {
				ch <- DigitalSection{Time: timeFlag, Data: dataList}
			}
			timeFlag = time
			dataList = make([]C.Digital, 0)
		}

		// 拼接数据, 并且添加到dataList
		digital := C.Digital{}
		digital.p_num = C.int32_t(pNum)
		digital.dv = C.bool(dv)
		digital.dvr = C.bool(dvr)
		digital.q = C.bool(q)
		digital.bf = C.bool(bf)
		digital.bq = C.bool(bq)
		digital.fai = C.bool(fai)
		digital.ms = C.bool(ms)
		digital.tew = C.char(tew)
		digital.cst = C.uint16_t(cst)

		dataList = append(dataList, digital)
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

// FastWriteRealtimeSection 极速写入实时断面
func FastWriteRealtimeSection(fastAnalogCh chan AnalogSection, fastDigitalCh chan DigitalSection, normalAnalogCh chan AnalogSection, normalDigitalCh chan DigitalSection, closeCh chan bool) {
	closeSum := 0
	for {
		select {
		case _ = <-fastAnalogCh:
			// GlobalDylib.DyWriteAnalog(section)
		case _ = <-fastDigitalCh:
			// GlobalDylib.DyWriteDigital(section)
		case section := <-normalAnalogCh:
			GlobalDylib.DyWriteAnalog(section)
		case _ = <-normalDigitalCh:
			// GlobalDylib.DyWriteDigital(section)
		case _ = <-closeCh:
			closeSum++
		}

		if closeSum == 2 {
			break
		}
	}
	close(closeCh)
}

func WriteHistorySection() {
}

func FastWrite(fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string) {
	fastAnalogCh := make(chan AnalogSection, CacheSize)
	fastDigitalCh := make(chan DigitalSection, CacheSize)
	close(fastAnalogCh)
	close(fastDigitalCh)
	normalAnalogCh := make(chan AnalogSection, CacheSize)
	normalDigitalCh := make(chan DigitalSection, CacheSize)
	closeCh := make(chan bool)
	wg := new(sync.WaitGroup)

	wg.Add(2)
	// go ReadAnalogCsv(fastAnalogCsvPath, fastAnalogCh, wg, closeCh)
	// go ReadDigitalCsv(fastDigitalCsvPath, fastDigitalCh, wg, closeCh)
	go ReadAnalogCsv(normalAnalogCsvPath, normalAnalogCh, wg, closeCh)
	go ReadDigitalCsv(normalDigitalCsvPath, normalDigitalCh, wg, closeCh)
	FastWriteRealtimeSection(fastAnalogCh, fastDigitalCh, normalAnalogCh, normalDigitalCh, closeCh)
	wg.Wait()
}

type DyLib struct {
	handle C.DYLIB_HANDLE
}

func NewDyLib(path string) *DyLib {
	return &DyLib{
		handle: C.load_library(C.CString(path)),
	}
}

func (df *DyLib) Login() {
	C.dy_login(df.handle)
}

func (df *DyLib) Logout() {
	C.dy_logout(df.handle)
}

var AA = atomic.Int32{}

func (df *DyLib) DyWriteAnalog(section AnalogSection) {
	if len(section.Data) == 0 {
		AA.Add(1)
		return
	}
	C.dy_write_analog(df.handle, C.int64_t(section.Time), (*C.Analog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *DyLib) DyWriteDigital(section DigitalSection) {
	if len(section.Data) == 0 {
		AA.Add(1)
		return
	}
	C.dy_write_digital(df.handle, C.int64_t(section.Time), (*C.Digital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *DyLib) DyWriteStaticAnalog(section StaticAnalogSection) {
	if len(section.Data) == 0 {
		AA.Add(1)
		return
	}
	C.dy_write_static_analog(df.handle, (*C.StaticAnalog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *DyLib) DyWriteStaticDigital(section StaticDigitalSection) {
	if len(section.Data) == 0 {
		AA.Add(1)
		return
	}
	C.dy_write_static_digital(df.handle, (*C.StaticDigital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

var GlobalDylib *DyLib = nil

func InitGlobalDylib(path string) {
	GlobalDylib = NewDyLib(path)
}

func main() {
	dyPath := "/Users/wangjingbo/Desktop/rtdb_writer/plugin_example/libcwrite_plugin.dylib"
	InitGlobalDylib(dyPath)

	wdDir, err := os.Getwd()
	if err != nil {
		panic("get word dir err")
	}
	fastAnalogCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_FAST_ANALOG.csv"
	fastDigitalCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_FAST_DIGITAL.csv"
	normalAnalogCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_NORMAL_ANALOG.csv"
	normalDigitalCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_NORMAL_DIGITAL.csv"
	FastWrite(fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath)
	fmt.Println(AA.Load())
}

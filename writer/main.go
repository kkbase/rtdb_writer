package main

// #cgo CFLAGS: -I../plugin
// #include "dylib.h"
// #include "write_plugin.h"
import "C"
import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

const CacheSize = 128

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

// ParseAnalogRecord 解析CSV行
func ParseAnalogRecord(record []string) (int64, C.Analog, error) {
	analog := C.Analog{}

	// 去除首行
	if record[0] == "TIME" {
		return -1, analog, errors.New("continue HEAD")
	}

	// 去除尾行
	if len(record) != 11 {
		return -1, analog, errors.New("continue TAIL")
	}

	// 解析行
	// TIME,P_NUM,AV,AVR,Q,BF,FQ,FAI,MS,TEW,CST
	time, err := strconv.ParseInt(record[0], 10, 64)
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse time error", record[0]))
	}

	pNum, err := strconv.ParseInt(record[1], 10, 32)
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse pNum error", record[1]))
	}

	av, err := strconv.ParseFloat(record[2], 64)
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse av error", record[2]))
	}
	avr, err := strconv.ParseFloat(record[3], 64)
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse avr error", record[3]))
	}

	q, err := strconv.ParseBool(record[4])
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse q error", record[4]))
	}

	bf, err := strconv.ParseBool(record[5])
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse bf error", record[5]))
	}

	qf, err := strconv.ParseBool(record[6])
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse qf error", record[6]))
	}

	fai, err := strconv.ParseFloat(record[7], 32)
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse fai error", record[7]))
	}

	ms, err := strconv.ParseBool(record[8])
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse ms error", record[8]))
	}

	if len(record[9]) != 1 {
		return -1, analog, errors.New(fmt.Sprintln("parse ms error", record[9]))
	}
	tew := record[9][0]

	cst, err := strconv.ParseInt(record[10], 10, 32)
	if err != nil {
		return -1, analog, errors.New(fmt.Sprintln("parse cst error", record[10]))
	}

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

	return time, analog, nil
}

// ParseDigitalRecord 解析CSV行
func ParseDigitalRecord(record []string) (int64, C.Digital, error) {
	digital := C.Digital{}

	// 去除首行
	if record[0] == "TIME" {
		return -1, digital, errors.New("continue HEAD")
	}

	// 去除尾行
	if len(record) != 11 {
		return -1, digital, errors.New("continue TAIL")
	}

	time, err := strconv.ParseInt(record[0], 10, 64)
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse time error", record[0]))
	}

	pNum, err := strconv.ParseInt(record[1], 10, 32)
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse pNum error", record[1]))
	}
	dv, err := strconv.ParseBool(record[2])
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse dv error", record[2]))
	}
	dvr, err := strconv.ParseBool(record[3])
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse dvr error", record[3]))
	}
	q, err := strconv.ParseBool(record[4])
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse q error", record[4]))
	}
	bf, err := strconv.ParseBool(record[5])
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse bf error", record[5]))
	}
	bq, err := strconv.ParseBool(record[6])
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse bq error", record[6]))
	}
	fai, err := strconv.ParseBool(record[7])
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse fai error", record[7]))
	}
	ms, err := strconv.ParseBool(record[8])
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse ms error", record[8]))
	}
	if len(record[9]) != 1 {
		return -1, digital, errors.New(fmt.Sprintln("parse ms error", record[9]))
	}
	tew := record[9][0]
	cst, err := strconv.ParseInt(record[10], 10, 32)
	if err != nil {
		return -1, digital, errors.New(fmt.Sprintln("parse cst error", record[10]))
	}

	// 拼接数据, 并且添加到dataList
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

	return time, digital, nil
}

// ReadAnalogCsv 读取CSV文件, 将其转换成 C.Analog 结构后发送到缓存队列
func ReadAnalogCsv(wg *sync.WaitGroup, closeCh chan struct{}, filepath string, ch chan AnalogSection) {
	defer wg.Done()

	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

	// CSV读取器
	reader := csv.NewReader(NewCRFilterReader(bufio.NewReader(file)))

	// 按行读取
	dataList := make([]C.Analog, 0)
	timeFlag := int64(-1)
	for {
		// 读取一行, 判断是否为EOF
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				if len(dataList) != 0 {
					fmt.Println("send: ", filepath, len(dataList))
					ch <- AnalogSection{Time: timeFlag, Data: dataList}
				}
				closeCh <- struct{}{}
				break
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		time, analog, err := ParseAnalogRecord(record)
		if err != nil {
			if !strings.Contains(err.Error(), "continue HEAD") {
				log.Printf("Error parsing record: %s", err)
			}
			continue
		}

		// time 初始化
		if timeFlag == -1 {
			timeFlag = time
		}

		// 如果出现的时间戳, 则更新timeFlag, 发送数据, 并且清空dataList
		if timeFlag != time {
			fmt.Println("send: ", filepath, len(dataList), "t1: ", timeFlag, "t2", time)
			ch <- AnalogSection{Time: timeFlag, Data: dataList}
			timeFlag = time
			dataList = make([]C.Analog, 0)
		}

		// dataList 插入
		dataList = append(dataList, analog)
	}

	// close(ch)
}

// ReadDigitalCsv 读取CSV文件, 将其转换成 C.Digital 结构后发送到缓存队列
func ReadDigitalCsv(wg *sync.WaitGroup, closeCh chan struct{}, filepath string, ch chan DigitalSection) {
	defer wg.Done()

	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

	// CSV读取器
	reader := csv.NewReader(NewCRFilterReader(bufio.NewReader(file)))

	// 按行读取
	dataList := make([]C.Digital, 0)
	timeFlag := int64(-1)
	for {
		// 读取一行, 判断是否为EOF
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				if len(dataList) != 0 {
					fmt.Println("send: ", filepath, len(dataList))
					ch <- DigitalSection{Time: timeFlag, Data: dataList}
				}
				closeCh <- struct{}{}
				break
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		time, digital, err := ParseDigitalRecord(record)
		if err != nil {
			if !strings.Contains(err.Error(), "continue HEAD") {
				log.Printf("Error parsing record: %s", err)
			}
			continue
		}

		// time 初始化
		if timeFlag == -1 {
			timeFlag = time
		}

		// 如果出现的时间戳, 则更新timeFlag, 发送数据, 并且清空dataList
		if timeFlag != time {
			if len(dataList) != 0 {
				fmt.Println("send: ", filepath, len(dataList), "t1: ", timeFlag, "t2", time)
				ch <- DigitalSection{Time: timeFlag, Data: dataList}
			}
			timeFlag = time
			dataList = make([]C.Digital, 0)
		}

		// dataList 插入
		dataList = append(dataList, digital)
	}
}

// ReadStaticAnalogCsv 读取CSV文件, 将其转换成 []C.StaticAnalog 切片
func ReadStaticAnalogCsv(_ string) StaticAnalogSection {
	return StaticAnalogSection{}
}

// ReadStaticDigitalCsv 读取CSV文件, 将其转换成 []C.StaticDigital 切片
func ReadStaticDigitalCsv(_ string) StaticDigitalSection {
	return StaticDigitalSection{}
}

// FastWriteRealtimeSection 极速写入实时断面
func FastWriteRealtimeSection(closeChan chan struct{}, fastAnalogCh chan AnalogSection, fastDigitalCh chan DigitalSection, normalAnalogCh chan AnalogSection, normalDigitalCh chan DigitalSection) {
	closeNum := 0
	for {
		select {
		case section := <-fastAnalogCh:
			GlobalDylib.DyWriteAnalog(section)
		case section := <-fastDigitalCh:
			GlobalDylib.DyWriteDigital(section)
		case section := <-normalAnalogCh:
			GlobalDylib.DyWriteAnalog(section)
		case section := <-normalDigitalCh:
			GlobalDylib.DyWriteDigital(section)
		case <-closeChan:
			closeNum++
		}

		if closeNum == 4 && len(fastAnalogCh) == 0 && len(fastDigitalCh) == 0 && len(normalAnalogCh) == 0 && len(normalDigitalCh) == 0 {
			break
		}
	}
	close(closeChan)
	close(fastAnalogCh)
	close(fastDigitalCh)
	close(normalAnalogCh)
	close(normalDigitalCh)
}

func WriteHistorySection() {
}

func FastWrite(fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string) {
	closeCh := make(chan struct{}, 4)
	fastAnalogCh := make(chan AnalogSection, CacheSize)
	fastDigitalCh := make(chan DigitalSection, CacheSize)
	normalAnalogCh := make(chan AnalogSection, CacheSize)
	normalDigitalCh := make(chan DigitalSection, CacheSize)
	wg := new(sync.WaitGroup)
	wg.Add(4)
	go ReadAnalogCsv(wg, closeCh, fastAnalogCsvPath, fastAnalogCh)
	go ReadDigitalCsv(wg, closeCh, fastDigitalCsvPath, fastDigitalCh)
	go ReadAnalogCsv(wg, closeCh, normalAnalogCsvPath, normalAnalogCh)
	go ReadDigitalCsv(wg, closeCh, normalDigitalCsvPath, normalDigitalCh)
	FastWriteRealtimeSection(closeCh, fastAnalogCh, fastDigitalCh, normalAnalogCh, normalDigitalCh)
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

func (df *DyLib) DyWriteAnalog(section AnalogSection) {
	C.dy_write_analog(df.handle, C.int64_t(section.Time), (*C.Analog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *DyLib) DyWriteDigital(section DigitalSection) {
	C.dy_write_digital(df.handle, C.int64_t(section.Time), (*C.Digital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *DyLib) DyWriteStaticAnalog(section StaticAnalogSection) {
	C.dy_write_static_analog(df.handle, (*C.StaticAnalog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *DyLib) DyWriteStaticDigital(section StaticDigitalSection) {
	C.dy_write_static_digital(df.handle, (*C.StaticDigital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

var GlobalDylib *DyLib = nil

func InitGlobalDylib(path string) {
	GlobalDylib = NewDyLib(path)
}

// CrFilterReader 是一个自定义的 io.Reader，用于去除数据流中的 \r 字符
type CrFilterReader struct {
	reader *bufio.Reader
}

// NewCRFilterReader 返回一个包装了 bufio.Reader 的 crFilterReader
func NewCRFilterReader(r *bufio.Reader) *CrFilterReader {
	return &CrFilterReader{reader: r}
}

// Read 实现了 io.Reader 接口，去除读取数据流中的 \r 字符
func (r *CrFilterReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if err != nil {
		return n, err
	}

	for i := 0; i < n; i++ {
		if p[i] == '\r' {
			p[i] = '\n'
		}
	}

	return n, nil
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
}

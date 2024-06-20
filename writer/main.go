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
	"time"
)

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
	ts, err := strconv.ParseInt(record[0], 10, 64)
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

	return ts, analog, nil
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

	ts, err := strconv.ParseInt(record[0], 10, 64)
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

	return ts, digital, nil
}

func ParseStaticAnalogRecord(record []string) (C.StaticAnalog, error) {
	staticAnalog := C.StaticAnalog{}

	// 去除首行
	if record[0] == "P_NUM" {
		return staticAnalog, errors.New("continue HEAD")
	}

	// 去除尾行
	if len(record) != 17 {
		return staticAnalog, errors.New("continue TAIL")
	}

	pNum, err := strconv.ParseInt(record[0], 10, 32)
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse pNum error", record[0]))
	}

	tagt, err := strconv.ParseInt(record[1], 10, 32)
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse tagt error", record[1]))
	}

	fack, err := strconv.ParseInt(record[2], 10, 32)
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse facl error", record[2]))
	}

	l4ar, err := strconv.ParseBool(record[3])
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse l4ar error", record[3]))
	}

	l3ar, err := strconv.ParseBool(record[4])
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse l3ar error", record[4]))
	}

	l2ar, err := strconv.ParseBool(record[5])
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse l2ar error", record[5]))
	}

	l1ar, err := strconv.ParseBool(record[6])
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse l1ar error", record[6]))
	}

	h4ar, err := strconv.ParseBool(record[7])
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse h4ar error", record[7]))
	}

	h3ar, err := strconv.ParseBool(record[8])
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse h3ar error", record[8]))
	}

	h2ar, err := strconv.ParseBool(record[9])
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse h2ar error", record[9]))
	}

	h1ar, err := strconv.ParseBool(record[10])
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse h1ar error", record[10]))
	}

	for i := 0; i < len(record[11]) && i < 32; i++ {
		staticAnalog.chn[i] = C.char(record[11][i])
	}

	for i := 0; i < len(record[12]) && i < 32; i++ {
		staticAnalog.pn[i] = C.char(record[12][i])
	}

	for i := 0; i < len(record[13]) && i < 128; i++ {
		staticAnalog.desc[i] = C.char(record[13][i])
	}

	for i := 0; i < len(record[14]) && i < 32; i++ {
		staticAnalog.unit[i] = C.char(record[14][i])
	}

	mu, err := strconv.ParseFloat(record[15], 32)
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse mu error", record[15]))
	}

	md, err := strconv.ParseFloat(record[16], 32)
	if err != nil {
		return staticAnalog, errors.New(fmt.Sprintln("parse md error", record[16]))
	}

	staticAnalog.p_num = C.int32_t(pNum)
	staticAnalog.tagt = C.uint16_t(tagt)
	staticAnalog.fack = C.uint16_t(fack)
	staticAnalog.l4ar = C.bool(l4ar)
	staticAnalog.l3ar = C.bool(l3ar)
	staticAnalog.l2ar = C.bool(l2ar)
	staticAnalog.l1ar = C.bool(l1ar)
	staticAnalog.h4ar = C.bool(h4ar)
	staticAnalog.h3ar = C.bool(h3ar)
	staticAnalog.h2ar = C.bool(h2ar)
	staticAnalog.h1ar = C.bool(h1ar)
	staticAnalog.mu = C.float(mu)
	staticAnalog.md = C.float(md)

	return staticAnalog, nil
}

func ParseStaticDigitalRecord(record []string) (C.StaticDigital, error) {
	staticDigital := C.StaticDigital{}

	// 去除首行
	if record[0] == "P_NUM" {
		return staticDigital, errors.New("continue HEAD")
	}

	// 去除尾行
	if len(record) != 6 {
		return staticDigital, errors.New("continue TAIL")
	}

	pNum, err := strconv.ParseInt(record[0], 10, 32)
	if err != nil {
		return staticDigital, errors.New(fmt.Sprintln("parse pNum error", record[0]))
	}

	fack, err := strconv.ParseInt(record[1], 10, 32)
	if err != nil {
		return staticDigital, errors.New(fmt.Sprintln("parse facl error", record[1]))
	}

	for i := 0; i < len(record[2]) && i < 32; i++ {
		staticDigital.chn[i] = C.char(record[2][i])
	}

	for i := 0; i < len(record[3]) && i < 32; i++ {
		staticDigital.pn[i] = C.char(record[3][i])
	}

	for i := 0; i < len(record[4]) && i < 128; i++ {
		staticDigital.desc[i] = C.char(record[4][i])
	}

	for i := 0; i < len(record[5]) && i < 32; i++ {
		staticDigital.unit[i] = C.char(record[5][i])
	}

	staticDigital.p_num = C.int32_t(pNum)
	staticDigital.fack = C.uint16_t(fack)

	return staticDigital, nil
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
	tsFlag := int64(-1)
	for {
		// 读取一行, 判断是否为EOF
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				if len(dataList) != 0 {
					ch <- AnalogSection{Time: tsFlag, Data: dataList}
				}
				closeCh <- struct{}{}
				break
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		ts, analog, err := ParseAnalogRecord(record)
		if err != nil {
			if !strings.Contains(err.Error(), "continue HEAD") {
				log.Printf("Error parsing record: %s", err)
			}
			continue
		}

		// time 初始化
		if tsFlag == -1 {
			tsFlag = ts
		}

		// 如果出现的时间戳, 则更新timeFlag, 发送数据, 并且清空dataList
		if tsFlag != ts {
			ch <- AnalogSection{Time: tsFlag, Data: dataList}
			tsFlag = ts
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
	tsFlag := int64(-1)
	for {
		// 读取一行, 判断是否为EOF
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				if len(dataList) != 0 {
					ch <- DigitalSection{Time: tsFlag, Data: dataList}
				}
				closeCh <- struct{}{}
				break
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		ts, digital, err := ParseDigitalRecord(record)
		if err != nil {
			if !strings.Contains(err.Error(), "continue HEAD") {
				log.Printf("Error parsing record: %s", err)
			}
			continue
		}

		// time 初始化
		if tsFlag == -1 {
			tsFlag = ts
		}

		// 如果出现的时间戳, 则更新timeFlag, 发送数据, 并且清空dataList
		if tsFlag != ts {
			if len(dataList) != 0 {
				ch <- DigitalSection{Time: tsFlag, Data: dataList}
			}
			tsFlag = ts
			dataList = make([]C.Digital, 0)
		}

		// dataList 插入
		dataList = append(dataList, digital)
	}
}

// ReadStaticAnalogCsv 读取CSV文件, 将其转换成 []C.StaticAnalog 切片
func ReadStaticAnalogCsv(filepath string) StaticAnalogSection {
	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

	// CSV读取器
	reader := csv.NewReader(NewCRFilterReader(bufio.NewReader(file)))

	dataList := make([]C.StaticAnalog, 0)
	for {
		// 读取一行, 判断是否为EOF
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		staticAnalog, err := ParseStaticAnalogRecord(record)
		if err != nil {
			if !strings.Contains(err.Error(), "continue HEAD") {
				log.Printf("Error parsing record: %s", err)
			}
			continue
		}

		dataList = append(dataList, staticAnalog)
	}

	return StaticAnalogSection{Data: dataList}
}

// ReadStaticDigitalCsv 读取CSV文件, 将其转换成 []C.StaticDigital 切片
func ReadStaticDigitalCsv(filepath string) StaticDigitalSection {
	// 打开文件
	file, err := os.Open(filepath)
	if err != nil {
		panic("can not open file: " + filepath)
	}
	defer func() { _ = file.Close() }()

	// CSV读取器
	reader := csv.NewReader(NewCRFilterReader(bufio.NewReader(file)))

	dataList := make([]C.StaticDigital, 0)
	for {
		// 读取一行, 判断是否为EOF
		record, err := reader.Read()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Printf("Error reading record: %s", err)
			continue
		}

		staticDigital, err := ParseStaticDigitalRecord(record)
		if err != nil {
			if !strings.Contains(err.Error(), "continue HEAD") {
				log.Printf("Error parsing record: %s", err)
			}
			continue
		}

		dataList = append(dataList, staticDigital)

	}

	return StaticDigitalSection{Data: dataList}
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

// PeriodicWriteRealtimeSection 周期性写入实时断面
func PeriodicWriteRealtimeSection(flag50ms bool, closeChan chan struct{}, fastAnalogCh chan AnalogSection, fastDigitalCh chan DigitalSection, normalAnalogCh chan AnalogSection, normalDigitalCh chan DigitalSection) {
	num := 0

	startWrite := time.Now()
	sleepDurationSum := time.Duration(0)
	for {
		num++

		start := time.Now()
		select {
		case section := <-fastAnalogCh:
			GlobalDylib.DyWriteAnalog(section)
		default:
		}
		select {
		case section := <-fastDigitalCh:
			GlobalDylib.DyWriteDigital(section)
		default:
		}
		if flag50ms {
			if num <= 2000 {
				if num%50 == 0 {
					select {
					case section := <-normalAnalogCh:
						GlobalDylib.DyWriteAnalog(section)
					default:
					}
					select {
					case section := <-normalDigitalCh:
						GlobalDylib.DyWriteDigital(section)
					default:
					}
				}
			} else {
				if num%400 == 0 {
					select {
					case section := <-normalAnalogCh:
						GlobalDylib.DyWriteAnalog(section)
					default:
					}
					select {
					case section := <-normalDigitalCh:
						GlobalDylib.DyWriteDigital(section)
					default:
					}
				}
			}
		} else {
			if num%400 == 0 {
				select {
				case section := <-normalAnalogCh:
					GlobalDylib.DyWriteAnalog(section)
				default:
				}
				select {
				case section := <-normalDigitalCh:
					GlobalDylib.DyWriteDigital(section)
				default:
				}
			}
		}
		end := time.Now()
		duration := end.Sub(start)
		if duration < time.Millisecond {
			sleepDuration := time.Millisecond - duration
			sleepDurationSum += sleepDuration
			time.Sleep(sleepDuration)
		}

		if len(closeChan) == 4 && len(fastAnalogCh) == 0 && len(fastDigitalCh) == 0 && len(normalAnalogCh) == 0 && len(normalDigitalCh) == 0 {
			break
		}
	}
	for i := 0; i < 4; i++ {
		<-closeChan
	}
	close(closeChan)
	close(fastAnalogCh)
	close(fastDigitalCh)
	close(normalAnalogCh)
	close(normalDigitalCh)

	endWrite := time.Now()
	allTime := endWrite.Sub(startWrite)
	writeTime := allTime - sleepDurationSum
	fmt.Println("周期性写入 - 写入时间:", writeTime, "睡眠时间: ", sleepDurationSum, "总时间: ", allTime)
}

// StaticWrite 静态写入
func StaticWrite(analogPath string, digitalPath string) {
	writeStart := time.Now()
	GlobalDylib.DyWriteStaticAnalog(ReadStaticAnalogCsv(analogPath))
	GlobalDylib.DyWriteStaticDigital(ReadStaticDigitalCsv(digitalPath))
	writeEnd := time.Now()
	fmt.Println("静态写入 - 写入耗时:", writeEnd.Sub(writeStart))
}

// FastWrite 极速写入
func FastWrite(fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string) {
	writeStart := time.Now()
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

	// 睡眠500毫秒, 等待协程加载缓存
	time.Sleep(500 * time.Millisecond)
	FastWriteRealtimeSection(closeCh, fastAnalogCh, fastDigitalCh, normalAnalogCh, normalDigitalCh)
	wg.Wait()
	writeEnd := time.Now()
	fmt.Println("极速写入 - 写入耗时:", writeEnd.Sub(writeStart))
}

// PeriodicWrite 周期性写入
func PeriodicWrite(flag50ms bool, fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string) {
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

	// 睡眠500毫秒, 等待协程加载缓存
	time.Sleep(500 * time.Millisecond)
	PeriodicWriteRealtimeSection(flag50ms, closeCh, fastAnalogCh, fastDigitalCh, normalAnalogCh, normalDigitalCh)
	wg.Wait()
}

// DyLib 动态库加载对象
// 用于加载插件, 内部调用了 plugin/dylib.h 头文件, 这个头文件封装了C的动态库加载函数
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

// Read 实现了 io.Reader 接口，替换数据流中的 '\r' 为 '\n'
// 备注: 因解析CSV文件时, 发现文件格式不标准, 有的CSV文件是以 "\r\r" 作为分隔符的, 所以统一替换成 '\n'
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
	wdDir, err := os.Getwd()
	if err != nil {
		panic("get word dir err")
	}

	staticAnalogCsvPath := wdDir + "/CSV20240614/1718350759143_HISTORY_NORMAL_STATIC_ANALOG.csv"
	staticDigitalCsvPath := wdDir + "/CSV20240614/1718350759143_HISTORY_NORMAL_STATIC_DIGITAL.csv"
	fastAnalogCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_FAST_ANALOG.csv"
	fastDigitalCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_FAST_DIGITAL.csv"
	normalAnalogCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_NORMAL_ANALOG.csv"
	normalDigitalCsvPath := wdDir + "/CSV20240614/1718350759143_REALTIME_NORMAL_DIGITAL.csv"
	dyPath := wdDir + "/plugin_example/libcwrite_plugin.dylib"

	// 加载动态库
	InitGlobalDylib(dyPath)

	// 静态写入
	StaticWrite(staticAnalogCsvPath, staticDigitalCsvPath)

	// 极速写入
	FastWrite(fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath)

	// 周期性写入(关闭前两秒50ms速写)
	PeriodicWrite(false, fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath)

	// 周期性写入(打开前两秒50ms速写)
	PeriodicWrite(true, fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath)
}

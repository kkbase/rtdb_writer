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
	"github.com/spf13/cobra"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CacheSize  缓存队列大小
const CacheSize = 64

// OverloadProtectionWriteDuration  过载保护持续时间, 2000毫秒(2秒)
const OverloadProtectionWriteDuration = 2000

// OverloadProtectionWritePeriodic 过载保护写入周期, 50毫秒
const OverloadProtectionWritePeriodic = 50

// FastRegularWritePeriodic 块采点写入周期, 1毫秒
const FastRegularWritePeriodic = 1

// NormalRegularWritePeriodic 普通点写入周期, 400毫秒
const NormalRegularWritePeriodic = 400

type WriteRtn struct {
	AllTime   time.Duration
	WriteTime time.Duration
	SleepTime time.Duration
	OtherTime time.Duration
}

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
func FastWriteRealtimeSection(unitNumber int64, closeChan chan struct{}, fastAnalogCh chan AnalogSection, fastDigitalCh chan DigitalSection, normalAnalogCh chan AnalogSection, normalDigitalCh chan DigitalSection) {
	closeNum := 0
	writeStart := time.Now()
	sleepDurationSum := time.Duration(0)
	for {
		select {
		case section := <-fastAnalogCh:
			wt := time.Now()
			GlobalPlugin.WriteRtAnalog(unitNumber, section)
			sleepDurationSum += time.Now().Sub(wt)
		case section := <-fastDigitalCh:
			wt := time.Now()
			GlobalPlugin.WriteRtDigital(unitNumber, section)
			sleepDurationSum += time.Now().Sub(wt)
		case section := <-normalAnalogCh:
			wt := time.Now()
			GlobalPlugin.WriteRtAnalog(unitNumber, section)
			sleepDurationSum += time.Now().Sub(wt)
		case section := <-normalDigitalCh:
			wt := time.Now()
			GlobalPlugin.WriteRtDigital(unitNumber, section)
			sleepDurationSum += time.Now().Sub(wt)
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

	allTime := time.Now().Sub(writeStart)
	fmt.Println("极速写入实时值 - 总耗时: ", allTime, "写入耗时:", sleepDurationSum, "其他耗时:", allTime-sleepDurationSum)
}

// FastWriteHisSection 极速写入历史断面
func FastWriteHisSection(unitNumber int64, closeChan chan struct{}, analogCh chan AnalogSection, digitalCh chan DigitalSection) {
	closeNum := 0
	writeStart := time.Now()
	sleepDurationSum := time.Duration(0)
	for {
		select {
		case section := <-analogCh:
			wt := time.Now()
			GlobalPlugin.WriteHisAnalog(unitNumber, section)
			sleepDurationSum += time.Now().Sub(wt)
		case section := <-digitalCh:
			wt := time.Now()
			GlobalPlugin.WriteHisDigital(unitNumber, section)
			sleepDurationSum += time.Now().Sub(wt)
		case <-closeChan:
			closeNum++
		}

		if closeNum == 2 && len(analogCh) == 0 && len(digitalCh) == 0 {
			break
		}
	}
	close(closeChan)
	close(analogCh)
	close(digitalCh)

	allTime := time.Now().Sub(writeStart)
	fmt.Println("极速写入历史值 - 总耗时: ", allTime, "写入耗时:", sleepDurationSum, "其他耗时:", allTime-sleepDurationSum)
}

// AsyncPeriodicWriteRtSection 周期性写入断面(实时/历史通用)
// unitNumber int64 机组数量
// overloadProtectionWriteDuration 过载保护持续时间, 单位毫秒
// overloadProtectionWritePeriodic 过载保护写入周期, 单位毫秒
// regularWritePeriodic 常规写入周期, 单位毫秒
// 返回值: 总时间, 写入时间, 睡眠时间
func AsyncPeriodicWriteRtSection(
	unitNumber int64,
	wg *sync.WaitGroup,
	rtnCh chan WriteRtn,
	overloadProtectionWriteDuration int,
	overloadProtectionWritePeriodic int,
	regularWritePeriodic int,
	closeChan chan struct{},
	analogCh chan AnalogSection,
	digitalCh chan DigitalSection,
) {
	defer wg.Done()

	sum := 0
	allStart := time.Now()
	writeDurationSum := time.Duration(0)
	sleepDurationSum := time.Duration(0)
	for {
		// 写入数据
		start := time.Now()
		select {
		case section := <-analogCh:
			GlobalPlugin.WriteRtAnalog(unitNumber, section)
		default:
		}
		select {
		case section := <-digitalCh:
			GlobalPlugin.WriteRtDigital(unitNumber, section)
		default:
		}
		duration := time.Now().Sub(start)
		writeDurationSum += duration

		// 全部写完, 退出循环
		if len(closeChan) == 2 && len(analogCh) == 0 && len(digitalCh) == 0 {
			break
		}

		// 睡眠剩余时间
		if sum < overloadProtectionWriteDuration {
			sum += overloadProtectionWritePeriodic

			if duration < time.Duration(overloadProtectionWritePeriodic)*time.Millisecond {
				sleepDuration := time.Duration(overloadProtectionWritePeriodic)*time.Millisecond - duration
				sleepDurationSum += sleepDuration
				time.Sleep(sleepDuration)
			}
		} else {
			if duration < time.Duration(regularWritePeriodic)*time.Millisecond {
				sleepDuration := time.Duration(regularWritePeriodic)*time.Millisecond - duration
				sleepDurationSum += sleepDuration
				time.Sleep(sleepDuration)
			}
		}
	}
	for i := 0; i < 2; i++ {
		<-closeChan
	}
	close(closeChan)
	close(analogCh)
	close(digitalCh)

	allTime := time.Now().Sub(allStart)
	writeTime := writeDurationSum
	sleepTime := sleepDurationSum
	otherTime := allTime - writeTime - sleepTime

	rtnCh <- WriteRtn{
		AllTime:   allTime,
		WriteTime: writeTime,
		SleepTime: sleepTime,
		OtherTime: otherTime,
	}
}

// StaticWrite 静态写入
func StaticWrite(unitNumber int64, analogPath string, digitalPath string) {
	writeStart := time.Now()
	GlobalPlugin.WriteStaticAnalog(unitNumber, ReadStaticAnalogCsv(analogPath))
	GlobalPlugin.WriteStaticDigital(unitNumber, ReadStaticDigitalCsv(digitalPath))
	writeEnd := time.Now()
	fmt.Println("静态写入 - 写入耗时:", writeEnd.Sub(writeStart))
}

// FastWriteRt 极速写入实时值
func FastWriteRt(unitNumber int64, fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string) {
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
	FastWriteRealtimeSection(unitNumber, closeCh, fastAnalogCh, fastDigitalCh, normalAnalogCh, normalDigitalCh)
	wg.Wait()
}

// PeriodicWriteRt 周期性写入实时值
func PeriodicWriteRt(unitNumber int64, overloadProtectionFlag bool, fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string) {
	fastCloseCh := make(chan struct{}, 2)
	normalCloseCh := make(chan struct{}, 2)
	fastAnalogCh := make(chan AnalogSection, CacheSize)
	fastDigitalCh := make(chan DigitalSection, CacheSize)
	normalAnalogCh := make(chan AnalogSection, CacheSize)
	normalDigitalCh := make(chan DigitalSection, CacheSize)
	wgRead := new(sync.WaitGroup)
	wgRead.Add(4)
	go ReadAnalogCsv(wgRead, fastCloseCh, fastAnalogCsvPath, fastAnalogCh)
	go ReadDigitalCsv(wgRead, fastCloseCh, fastDigitalCsvPath, fastDigitalCh)
	go ReadAnalogCsv(wgRead, normalCloseCh, normalAnalogCsvPath, normalAnalogCh)
	go ReadDigitalCsv(wgRead, normalCloseCh, normalDigitalCsvPath, normalDigitalCh)

	// 睡眠500毫秒, 等待协程加载缓存
	time.Sleep(500 * time.Millisecond)
	fastRtnCh := make(chan WriteRtn, 1)
	normalRtnCh := make(chan WriteRtn, 1)
	wgWrite := new(sync.WaitGroup)
	wgWrite.Add(2)
	if overloadProtectionFlag {
		go AsyncPeriodicWriteRtSection(unitNumber, wgWrite, fastRtnCh, 0, 0, FastRegularWritePeriodic, fastCloseCh, fastAnalogCh, fastDigitalCh)
		go AsyncPeriodicWriteRtSection(unitNumber, wgWrite, normalRtnCh, OverloadProtectionWriteDuration, OverloadProtectionWritePeriodic, NormalRegularWritePeriodic, normalCloseCh, normalAnalogCh, normalDigitalCh)
	} else {
		go AsyncPeriodicWriteRtSection(unitNumber, wgWrite, fastRtnCh, 0, 0, FastRegularWritePeriodic, fastCloseCh, fastAnalogCh, fastDigitalCh)
		go AsyncPeriodicWriteRtSection(unitNumber, wgWrite, normalRtnCh, 0, 0, NormalRegularWritePeriodic, normalCloseCh, normalAnalogCh, normalDigitalCh)
	}
	wgWrite.Wait()
	wgRead.Wait()

	fastRtn := <-fastRtnCh
	normalRtn := <-normalRtnCh
	if overloadProtectionFlag {
		fmt.Println("周期性写入实时值(开启载保护):")
	} else {
		fmt.Println("周期性写入实时值(关闭过载保护):")
	}
	fmt.Println("快采点 - 总耗时:", fastRtn.AllTime, "写入耗时:", fastRtn.WriteTime, "睡眠耗时:", fastRtn.SleepTime, "其他耗时:", fastRtn.OtherTime)
	fmt.Println("普通点 - 总耗时:", normalRtn.AllTime, "写入耗时:", normalRtn.WriteTime, "睡眠耗时:", normalRtn.SleepTime, "其他耗时:", normalRtn.OtherTime)
	close(fastRtnCh)
	close(normalRtnCh)
}

// FastWriteHis 极速写历史
func FastWriteHis(unitNumber int64, analogCsvPath string, digitalCsvPath string) {
	closeCh := make(chan struct{}, 4)
	analogCh := make(chan AnalogSection, CacheSize)
	digitalCh := make(chan DigitalSection, CacheSize)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go ReadAnalogCsv(wg, closeCh, analogCsvPath, analogCh)
	go ReadDigitalCsv(wg, closeCh, digitalCsvPath, digitalCh)

	// 睡眠500毫秒, 等待协程加载缓存
	time.Sleep(500 * time.Millisecond)
	FastWriteHisSection(unitNumber, closeCh, analogCh, digitalCh)
	wg.Wait()
}

// PeriodicWriteHis 周期性写历史
func PeriodicWriteHis(unitNumber int64, analogCsvPath string, digitalCsvPath string) {
	normalCloseCh := make(chan struct{}, 2)
	normalAnalogCh := make(chan AnalogSection, CacheSize)
	normalDigitalCh := make(chan DigitalSection, CacheSize)
	wgRead := new(sync.WaitGroup)
	wgRead.Add(2)
	go ReadAnalogCsv(wgRead, normalCloseCh, analogCsvPath, normalAnalogCh)
	go ReadDigitalCsv(wgRead, normalCloseCh, digitalCsvPath, normalDigitalCh)

	// 睡眠500毫秒, 等待协程加载缓存
	time.Sleep(500 * time.Millisecond)
	rtnCh := make(chan WriteRtn, 1)
	wgWrite := new(sync.WaitGroup)
	wgWrite.Add(1)
	go AsyncPeriodicWriteRtSection(unitNumber, wgWrite, rtnCh, 0, 0, NormalRegularWritePeriodic, normalCloseCh, normalAnalogCh, normalDigitalCh)
	wgWrite.Wait()
	wgRead.Wait()

	rtn := <-rtnCh
	fmt.Println("周期性写入历史值: 普通点 - 总耗时:", rtn.AllTime, "写入耗时:", rtn.WriteTime, "睡眠耗时:", rtn.SleepTime, "其他耗时:", rtn.OtherTime)
	close(rtnCh)
}

// WritePlugin 写入插件
// 用于加载插件, 内部调用了 plugin/dylib.h 头文件, 这个头文件封装了C的动态库加载函数
type WritePlugin struct {
	handle C.DYLIB_HANDLE
}

func NewWritePlugin(path string) *WritePlugin {
	return &WritePlugin{
		handle: C.load_library(C.CString(path)),
	}
}

func (df *WritePlugin) Login() {
	C.dy_login(df.handle)
}

func (df *WritePlugin) Logout() {
	C.dy_logout(df.handle)
}

func (df *WritePlugin) WriteRtAnalog(unitNumber int64, section AnalogSection) {
	if unitNumber == 1 {
		df.SyncWriteRtAnalog(0, section)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteRtAnalog(wg, i, section)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteRtDigital(unitNumber int64, section DigitalSection) {
	if unitNumber == 1 {
		df.SyncWriteRtDigital(0, section)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteRtDigital(wg, i, section)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteHisAnalog(unitNumber int64, section AnalogSection) {
	if unitNumber == 1 {
		df.SyncWriteHisAnalog(0, section)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteHisAnalog(wg, i, section)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteHisDigital(unitNumber int64, section DigitalSection) {
	if unitNumber == 1 {
		df.SyncWriteHisDigital(0, section)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteHisDigital(wg, i, section)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteStaticAnalog(unitNumber int64, section StaticAnalogSection) {
	if unitNumber == 1 {
		df.SyncWriteStaticAnalog(0, section)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteStaticAnalog(wg, i, section)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteStaticDigital(unitNumber int64, section StaticDigitalSection) {
	if unitNumber == 1 {
		df.SyncWriteStaticDigital(0, section)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteStaticDigital(wg, i, section)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) SyncWriteRtAnalog(unitId int64, section AnalogSection) {
	C.dy_write_rt_analog(df.handle, C.int64_t(unitId), C.int64_t(section.Time), (*C.Analog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) SyncWriteRtDigital(unitId int64, section DigitalSection) {
	C.dy_write_rt_digital(df.handle, C.int64_t(unitId), C.int64_t(section.Time), (*C.Digital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) SyncWriteHisAnalog(unitId int64, section AnalogSection) {
	C.dy_write_his_analog(df.handle, C.int64_t(unitId), C.int64_t(section.Time), (*C.Analog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) SyncWriteHisDigital(unitId int64, section DigitalSection) {
	C.dy_write_his_digital(df.handle, C.int64_t(unitId), C.int64_t(section.Time), (*C.Digital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) SyncWriteStaticAnalog(unitId int64, section StaticAnalogSection) {
	C.dy_write_static_analog(df.handle, C.int64_t(unitId), (*C.StaticAnalog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) SyncWriteStaticDigital(unitId int64, section StaticDigitalSection) {
	C.dy_write_static_digital(df.handle, C.int64_t(unitId), (*C.StaticDigital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) AsyncWriteRtAnalog(wg *sync.WaitGroup, unitId int64, section AnalogSection) {
	defer wg.Done()
	C.dy_write_rt_analog(df.handle, C.int64_t(unitId), C.int64_t(section.Time), (*C.Analog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) AsyncWriteRtDigital(wg *sync.WaitGroup, unitId int64, section DigitalSection) {
	defer wg.Done()
	C.dy_write_rt_digital(df.handle, C.int64_t(unitId), C.int64_t(section.Time), (*C.Digital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) AsyncWriteHisAnalog(wg *sync.WaitGroup, unitId int64, section AnalogSection) {
	defer wg.Done()
	C.dy_write_his_analog(df.handle, C.int64_t(unitId), C.int64_t(section.Time), (*C.Analog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) AsyncWriteHisDigital(wg *sync.WaitGroup, unitId int64, section DigitalSection) {
	defer wg.Done()
	C.dy_write_his_digital(df.handle, C.int64_t(unitId), C.int64_t(section.Time), (*C.Digital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) AsyncWriteStaticAnalog(wg *sync.WaitGroup, unitId int64, section StaticAnalogSection) {
	defer wg.Done()
	C.dy_write_static_analog(df.handle, C.int64_t(unitId), (*C.StaticAnalog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) AsyncWriteStaticDigital(wg *sync.WaitGroup, unitId int64, section StaticDigitalSection) {
	defer wg.Done()
	C.dy_write_static_digital(df.handle, C.int64_t(unitId), (*C.StaticDigital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

var GlobalPlugin *WritePlugin = nil

func InitGlobalPlugin(path string) {
	GlobalPlugin = NewWritePlugin(path)
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

var rootCmd = &cobra.Command{
	Use:   "Rtdb Writer",
	Short: "RTDB/TSDB performance testing tool",
	Run: func(cmd *cobra.Command, args []string) {
		_ = cmd.Help()
	},
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Rtdb Writer version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("v0.1.0")
	},
}

var staticWrite = &cobra.Command{
	Use:   "static_write",
	Short: "Write STATIC_ANALOG.csv, STATIC_DIGITAL.csv",
	Run: func(cmd *cobra.Command, args []string) {
		pluginPath, _ := cmd.Flags().GetString("plugin")
		staticAnalogCsvPath, _ := cmd.Flags().GetString("static_analog")
		staticDigitalCsvPath, _ := cmd.Flags().GetString("static_digital")
		unitNumber, _ := cmd.Flags().GetInt64("unit_number")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login()

		// 静态写入
		StaticWrite(unitNumber, staticAnalogCsvPath, staticDigitalCsvPath)

		// 登出
		GlobalPlugin.Logout()
	},
}

var rtFastWrite = &cobra.Command{
	Use:   "rt_fast_write",
	Short: "Fast Write REALTIME_FAST_ANALOG.csv, REALTIME_FAST_DIGITAL.csv, REALTIME_NORMAL_ANALOG.csv, REALTIME_NORMAL_DIGITAL.csv",
	Run: func(cmd *cobra.Command, args []string) {
		pluginPath, _ := cmd.Flags().GetString("plugin")
		fastAnalogCsvPath, _ := cmd.Flags().GetString("rt_fast_analog")
		fastDigitalCsvPath, _ := cmd.Flags().GetString("rt_fast_digital")
		normalAnalogCsvPath, _ := cmd.Flags().GetString("rt_normal_analog")
		normalDigitalCsvPath, _ := cmd.Flags().GetString("rt_normal_digital")
		unitNumber, _ := cmd.Flags().GetInt64("unit_number")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login()

		// 极速写入
		FastWriteRt(unitNumber, fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath)

		// 登出
		GlobalPlugin.Logout()
	},
}

var hisFastWrite = &cobra.Command{
	Use:   "his_fast_write",
	Short: "Fast Write HISTORY_NORMAL_ANALOG.csv, HISTORY_NORMAL_DIGITAL.csv",
	Run: func(cmd *cobra.Command, args []string) {
		pluginPath, _ := cmd.Flags().GetString("plugin")
		analogCsvPath, _ := cmd.Flags().GetString("his_normal_analog")
		digitalCsvPath, _ := cmd.Flags().GetString("his_normal_digital")
		unitNumber, _ := cmd.Flags().GetInt64("unit_number")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login()

		// 极速写入历史
		FastWriteHis(unitNumber, analogCsvPath, digitalCsvPath)

		// 登出
		GlobalPlugin.Logout()
	},
}

var hisPeriodicWrite = &cobra.Command{
	Use:   "his_periodic_write",
	Short: "Periodic Write HISTORY_NORMAL_ANALOG.csv, HISTORY_NORMAL_DIGITAL.csv",
	Run: func(cmd *cobra.Command, args []string) {
		pluginPath, _ := cmd.Flags().GetString("plugin")
		analogCsvPath, _ := cmd.Flags().GetString("his_normal_analog")
		digitalCsvPath, _ := cmd.Flags().GetString("his_normal_digital")
		unitNumber, _ := cmd.Flags().GetInt64("unit_number")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login()

		// 周期性写入
		PeriodicWriteHis(unitNumber, analogCsvPath, digitalCsvPath)

		// 登入
		GlobalPlugin.Logout()
	},
}

var rtPeriodicWrite = &cobra.Command{
	Use:   "rt_periodic_write",
	Short: "Periodic Write REALTIME_FAST_ANALOG.csv, REALTIME_FAST_DIGITAL.csv, REALTIME_NORMAL_ANALOG.csv, REALTIME_NORMAL_DIGITAL.csv",
	Run: func(cmd *cobra.Command, args []string) {
		pluginPath, _ := cmd.Flags().GetString("plugin")
		overloadProtection, _ := cmd.Flags().GetBool("overload_protection")
		fastAnalogCsvPath, _ := cmd.Flags().GetString("rt_fast_analog")
		fastDigitalCsvPath, _ := cmd.Flags().GetString("rt_fast_digital")
		normalAnalogCsvPath, _ := cmd.Flags().GetString("rt_normal_analog")
		normalDigitalCsvPath, _ := cmd.Flags().GetString("rt_normal_digital")
		unitNumber, _ := cmd.Flags().GetInt64("unit_number")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login()

		// 周期性写入
		PeriodicWriteRt(unitNumber, overloadProtection, fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath)

		// 登入
		GlobalPlugin.Logout()
	},
}

func init() {
	rootCmd.CompletionOptions.DisableDefaultCmd = true

	rootCmd.AddCommand(versionCmd)

	rootCmd.AddCommand(staticWrite)
	staticWrite.Flags().StringP("plugin", "", "", "plugin path")
	staticWrite.Flags().StringP("static_analog", "", "", "static analog csv path")
	staticWrite.Flags().StringP("static_digital", "", "", "static digital csv path")
	staticWrite.Flags().Int64P("unit_number", "", 1, "unit number")

	rootCmd.AddCommand(rtFastWrite)
	rtFastWrite.Flags().StringP("plugin", "", "", "plugin path")
	rtFastWrite.Flags().StringP("rt_fast_analog", "", "", "realtime fast analog csv path")
	rtFastWrite.Flags().StringP("rt_fast_digital", "", "", "realtime fast digital csv path")
	rtFastWrite.Flags().StringP("rt_normal_analog", "", "", "realtime normal analog csv path")
	rtFastWrite.Flags().StringP("rt_normal_digital", "", "", "realtime normal digital csv path")
	rtFastWrite.Flags().Int64P("unit_number", "", 1, "unit number")

	rootCmd.AddCommand(hisFastWrite)
	hisFastWrite.Flags().StringP("plugin", "", "", "plugin path")
	hisFastWrite.Flags().StringP("his_normal_analog", "", "", "history normal analog csv path")
	hisFastWrite.Flags().StringP("his_normal_digital", "", "", "history normal digital csv path")
	hisFastWrite.Flags().Int64P("unit_number", "", 1, "unit number")

	rootCmd.AddCommand(hisPeriodicWrite)
	hisPeriodicWrite.Flags().StringP("plugin", "", "", "plugin path")
	hisPeriodicWrite.Flags().StringP("his_normal_analog", "", "", "history normal analog csv path")
	hisPeriodicWrite.Flags().StringP("his_normal_digital", "", "", "history normal digital csv path")
	hisPeriodicWrite.Flags().Int64P("unit_number", "", 1, "unit number")

	rootCmd.AddCommand(rtPeriodicWrite)
	rtPeriodicWrite.Flags().StringP("plugin", "", "", "plugin path")
	rtPeriodicWrite.Flags().BoolP("overload_protection", "", false, "overload protection flag")
	rtPeriodicWrite.Flags().StringP("rt_fast_analog", "", "", "realtime fast analog csv path")
	rtPeriodicWrite.Flags().StringP("rt_fast_digital", "", "", "realtime fast digital csv path")
	rtPeriodicWrite.Flags().StringP("rt_normal_analog", "", "", "realtime normal analog csv path")
	rtPeriodicWrite.Flags().StringP("rt_normal_digital", "", "", "realtime normal digital csv path")
	rtPeriodicWrite.Flags().Int64P("unit_number", "", 1, "unit number")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	Execute()
}

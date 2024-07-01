package main

// #cgo CFLAGS: -I../plugin
// #include <stdlib.h>
// #include "dylib.h"
// #include "write_plugin.h"
import "C"
import (
	"bufio"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/spf13/cobra"
	"gonum.org/v1/gonum/stat"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
)

// CacheSize  缓存队列大小
const CacheSize = 64

// OverloadProtectionWriteDuration  过载保护持续时间, 2000毫秒(2秒)
const OverloadProtectionWriteDuration = 2000

// OverloadProtectionWritePeriodic 过载保护写入周期, 50毫秒
const OverloadProtectionWritePeriodic = 50

// FastRegularWritePeriodic 快采点写入周期, 1毫秒
const FastRegularWritePeriodic = 1

// NormalRegularWritePeriodic 普通点写入周期, 400毫秒
const NormalRegularWritePeriodic = 400

// WriteSectionInfo  每次写入断面, 记录基本信息
type WriteSectionInfo struct {
	UnitNumber   int64         // 机组数量
	Time         int64         // 断面时间
	Duration     time.Duration // 写入断面消耗的时间
	SectionCount int64         // 断面数量
	PNumCount    int64         // PNum数量
}

var FastAnalogWriteSectionInfoList = make([]WriteSectionInfo, 0)
var FastDigitalWriteSectionInfoList = make([]WriteSectionInfo, 0)
var NormalAnalogWriteSectionInfoList = make([]WriteSectionInfo, 0)
var NormalDigitalWriteSectionInfoList = make([]WriteSectionInfo, 0)
var FastSleepDurationList = make([]time.Duration, 0)
var NormalSleepDurationList = make([]time.Duration, 0)

func DurationListToFloatList(durationList []time.Duration) []float64 {
	rtn := make([]float64, 0)
	for _, t := range durationList {
		rtn = append(rtn, float64(t))
	}
	return rtn
}

func Summary(analogList []WriteSectionInfo, digitalList []WriteSectionInfo) (time.Duration, int, time.Duration, time.Duration, time.Duration, time.Duration, time.Duration, time.Duration, int) {
	infoList := make([]WriteSectionInfo, 0)

	for _, info := range analogList {
		infoList = append(infoList, WriteSectionInfo{
			Duration:     info.Duration,
			SectionCount: info.SectionCount,
			PNumCount:    info.PNumCount,
			Time:         info.Time,
			UnitNumber:   info.UnitNumber,
		})
	}

	for i, info := range digitalList {
		if i < len(infoList) {
			infoList[i].Duration += info.Duration
			infoList[i].PNumCount += info.PNumCount
		}
	}

	allDuration := time.Duration(0)
	sectionCount := 0
	durationList := make([]time.Duration, 0)
	pnumCount := 0
	for _, info := range infoList {
		durationList = append(durationList, info.Duration)
		allDuration += info.Duration
		sectionCount += int(info.SectionCount)
		pnumCount += int(info.PNumCount)
	}

	sort.Slice(durationList, func(i, j int) bool {
		return durationList[i] < durationList[j]
	})
	dAvg := allDuration / time.Duration(sectionCount)
	dMax := time.Duration(stat.Quantile(1.00, stat.Empirical, DurationListToFloatList(durationList), nil))
	dMin := time.Duration(stat.Quantile(0.00, stat.Empirical, DurationListToFloatList(durationList), nil))
	dP99 := time.Duration(stat.Quantile(0.99, stat.Empirical, DurationListToFloatList(durationList), nil))
	dP95 := time.Duration(stat.Quantile(0.95, stat.Empirical, DurationListToFloatList(durationList), nil))
	dP50 := time.Duration(stat.Quantile(0.50, stat.Empirical, DurationListToFloatList(durationList), nil))

	return allDuration, sectionCount, dAvg, dMax, dMin, dP99, dP95, dP50, pnumCount
}

func StaticSummary(name string, start time.Time, end time.Time, analog []WriteSectionInfo, digital []WriteSectionInfo) {
	fmt.Printf("%v - 开始时间: %v, 结束时间: %v\n", name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	fmt.Printf("总耗时: %v, 机组数量: %v, 写入pnum数量: %v\n", analog[0].Duration+digital[0].Duration, analog[0].UnitNumber, analog[0].PNumCount+digital[0].PNumCount)
}

func HisFastWriteSummary(
	name string, start time.Time, end time.Time,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo,
) {
	nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)
	fmt.Printf("%v - 开始时间: %v, 结束时间: %v\n", name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	fmt.Printf("总耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v,\n\t\t最长耗时: %v, 最短耗时: %v, 中位数耗时: %v, P99耗时: %v, P95耗时: %v\n",
		nAll, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
	)
}

func RtFastWriteSummary(
	name string, start time.Time, end time.Time,
	fastAnalog []WriteSectionInfo, fastDigital []WriteSectionInfo,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo,
) {
	fAll, fCount, fAvg, fMax, fMin, fP99, fP95, fP50, fPNum := Summary(fastAnalog, fastDigital)
	nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)

	fmt.Printf("%v - 开始时间: %v, 结束时间: %v\n", name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	fmt.Printf("写入总耗时: %v\n", fAll+nAll)
	fmt.Printf("快采点 - 总耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v, \n\t\t最长耗时: %v, 最短耗时: %v, 中位数耗时: %v, P99耗时: %v, P95耗时: %v\n",
		fAll, fCount, fPNum, fAvg, fMax, fMin, fP99, fP95, fP50,
	)
	fmt.Printf("普通点 - 总耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v, \n\t\t最长耗时: %v, 最短耗时: %v, 中位数耗时: %v, P99耗时: %v, P95耗时: %v\n",
		nAll, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
	)
}

func PeriodicWriteHisSummary(
	name string, start time.Time, end time.Time,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo, normalSleepList []time.Duration,
) {
	nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)
	nSleepSum := time.Duration(0)
	for _, d := range normalSleepList {
		nSleepSum += d
	}
	fmt.Printf("%v - 开始时间: %v, 结束时间: %v\n", name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	fmt.Printf("总耗时: %v, 睡眠耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v, \n\t\t最长耗时: %v, 最短耗时: %v, 中位数耗时: %v, P99耗时: %v, P95耗时: %v\n",
		nAll, nSleepSum, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
	)
}

func PeriodicWriteRtSummary(
	name string, start time.Time, end time.Time,
	fastAnalog []WriteSectionInfo, fastDigital []WriteSectionInfo, fastSleepList []time.Duration,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo, normalSleepList []time.Duration,
) {
	fAll, fCount, fAvg, fMax, fMin, fP99, fP95, fP50, fPNum := Summary(fastAnalog, fastDigital)
	nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)
	fSleepSum := time.Duration(0)
	for _, d := range fastSleepList {
		fSleepSum += d
	}
	nSleepSum := time.Duration(0)
	for _, d := range normalSleepList {
		nSleepSum += d
	}
	fmt.Printf("%v - 开始时间: %v, 结束时间: %v\n", name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	fmt.Printf("快采点 - 总耗时: %v, 睡眠耗时: %v, 断面数量: %v, PNUM数量: %v, \n\t\t平均耗时: %v ,最长耗时: %v, 最短耗时: %v, 中位数耗时: %v, P99耗时: %v, P95耗时: %v\n",
		fAll, fSleepSum, fCount, fPNum, fAvg, fMax, fMin, fP99, fP95, fP50,
	)
	fmt.Printf("常规点 - 总耗时: %v, 睡眠耗时: %v, 断面数量: %v, PNUM数量: %v, \n\t\t平均耗时: %v ,最长耗时: %v, 最短耗时: %v, 中位数耗时: %v, P99耗时: %v, P95耗时: %v\n",
		nAll, nSleepSum, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
	)
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
func ReadAnalogCsv(wg *sync.WaitGroup, closeCh chan struct{}, filepath string, ch chan AnalogSection, exitCh chan bool) {
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
		if len(exitCh) != 0 {
			fmt.Println("信号中断CSV读取协程:", filepath)
			return
		}

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
}

// ReadDigitalCsv 读取CSV文件, 将其转换成 C.Digital 结构后发送到缓存队列
func ReadDigitalCsv(wg *sync.WaitGroup, closeCh chan struct{}, filepath string, ch chan DigitalSection, exitCh chan bool) {
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
		if len(exitCh) != 0 {
			fmt.Println("信号中断CSV读取协程:", filepath)
			return
		}

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
func FastWriteRealtimeSection(unitNumber int64, closeChan chan struct{}, fastAnalogCh chan AnalogSection, fastDigitalCh chan DigitalSection, normalAnalogCh chan AnalogSection, normalDigitalCh chan DigitalSection, exitCh chan bool) {
	closeNum := 0

	for {
		select {
		case <-exitCh:
			if len(fastAnalogCh) != 0 {
				for i := 0; i < len(fastAnalogCh); i++ {
					<-fastAnalogCh
				}
			}
			if len(fastDigitalCh) != 0 {
				for i := 0; i < len(fastDigitalCh); i++ {
					<-fastDigitalCh
				}
			}
			if len(normalAnalogCh) != 0 {
				for i := 0; i < len(normalAnalogCh); i++ {
					<-normalAnalogCh
				}
			}
			if len(normalDigitalCh) != 0 {
				for i := 0; i < len(normalDigitalCh); i++ {
					<-normalDigitalCh
				}
			}
			return
		case section := <-fastAnalogCh:
			wt := time.Now()
			GlobalPlugin.WriteRtAnalog(unitNumber, section)
			FastAnalogWriteSectionInfoList = append(FastAnalogWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.Time,
				Duration:     time.Now().Sub(wt),
				SectionCount: 1,
				PNumCount:    int64(len(section.Data)),
			})
		case section := <-fastDigitalCh:
			wt := time.Now()
			GlobalPlugin.WriteRtDigital(unitNumber, section)
			FastDigitalWriteSectionInfoList = append(FastDigitalWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.Time,
				Duration:     time.Now().Sub(wt),
				SectionCount: 1,
				PNumCount:    int64(len(section.Data)),
			})
		case section := <-normalAnalogCh:
			wt := time.Now()
			GlobalPlugin.WriteRtAnalog(unitNumber, section)
			NormalAnalogWriteSectionInfoList = append(NormalAnalogWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.Time,
				Duration:     time.Now().Sub(wt),
				SectionCount: 1,
				PNumCount:    int64(len(section.Data)),
			})
		case section := <-normalDigitalCh:
			wt := time.Now()
			GlobalPlugin.WriteRtDigital(unitNumber, section)
			NormalDigitalWriteSectionInfoList = append(NormalDigitalWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.Time,
				Duration:     time.Now().Sub(wt),
				SectionCount: 1,
				PNumCount:    int64(len(section.Data)),
			})
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

// FastWriteHisSection 极速写入历史断面
func FastWriteHisSection(unitNumber int64, closeChan chan struct{}, analogCh chan AnalogSection, digitalCh chan DigitalSection, exitCh chan bool) {
	closeNum := 0
	for {
		if len(exitCh) != 0 {
			if len(analogCh) != 0 {
				for i := 0; i < len(analogCh); i++ {
					<-analogCh
				}
			}
			if len(digitalCh) != 0 {
				for i := 0; i < len(digitalCh); i++ {
					<-digitalCh
				}
			}
			return
		}

		select {
		case section := <-analogCh:
			wt := time.Now()
			GlobalPlugin.WriteHisAnalog(unitNumber, section)
			NormalAnalogWriteSectionInfoList = append(NormalAnalogWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.Time,
				Duration:     time.Now().Sub(wt),
				SectionCount: 1,
				PNumCount:    int64(len(section.Data)),
			})
		case section := <-digitalCh:
			wt := time.Now()
			GlobalPlugin.WriteHisDigital(unitNumber, section)
			NormalDigitalWriteSectionInfoList = append(NormalDigitalWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.Time,
				Duration:     time.Now().Sub(wt),
				SectionCount: 1,
				PNumCount:    int64(len(section.Data)),
			})
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
}

// AsyncPeriodicWriteSection 周期性写入断面(实时/历史通用)
// unitNumber int64 机组数量
// overloadProtectionWriteDuration 过载保护持续时间, 单位毫秒
// overloadProtectionWritePeriodic 过载保护写入周期, 单位毫秒
// regularWritePeriodic 常规写入周期, 单位毫秒
// 返回值: 总时间, 写入时间, 睡眠时间
func AsyncPeriodicWriteSection(
	unitNumber int64,
	wg *sync.WaitGroup,
	overloadProtectionWriteDuration int,
	overloadProtectionWritePeriodic int,
	regularWritePeriodic int,
	closeChan chan struct{},
	analogCh chan AnalogSection,
	digitalCh chan DigitalSection,
	isRt bool,
	isFast bool,
	fastCache bool,
	exitCh chan bool,
) {
	defer wg.Done()

	sum := 0
	for {
		if len(exitCh) != 0 {
			if len(analogCh) != 0 {
				for i := 0; i < len(analogCh); i++ {
					<-analogCh
				}
			}
			if len(digitalCh) != 0 {
				for i := 0; i < len(digitalCh); i++ {
					<-digitalCh
				}
			}
			return
		}

		if fastCache {
			analogList := make([]AnalogSection, 0)
			digitalList := make([]DigitalSection, 0)
			for {
				if len(analogList) < 100 {
					select {
					case section := <-analogCh:
						analogList = append(analogList, section)
					default:
					}
				}
				if len(digitalList) < 100 {
					select {
					case section := <-digitalCh:
						digitalList = append(digitalList, section)
					default:
					}
				}
				if len(analogList) == 100 && len(digitalList) == 100 {
					break
				}
				if len(closeChan) == 2 {
					for i := 0; i < len(analogCh); i++ {
						section := <-analogCh
						analogList = append(analogList, section)
					}
					for i := 0; i < len(digitalCh); i++ {
						section := <-digitalCh
						digitalList = append(digitalList, section)
					}
					break
				}
			}
			start := time.Now()

			GlobalPlugin.WriteRtAnalogList(unitNumber, analogList)
			GlobalPlugin.WriteRtDigitalList(unitNumber, digitalList)

			duration := time.Now().Sub(start)
			// 全部写完, 退出循环
			if len(closeChan) == 2 && len(analogCh) == 0 && len(digitalCh) == 0 {
				break
			}

			// 睡眠
			if duration < time.Duration(regularWritePeriodic)*time.Millisecond*100 {
				sleepDuration := time.Duration(regularWritePeriodic)*time.Millisecond*100 - duration
				if isFast {
					FastSleepDurationList = append(FastSleepDurationList, sleepDuration)
				} else {
					NormalSleepDurationList = append(NormalSleepDurationList, sleepDuration)
				}
				time.Sleep(sleepDuration)
			}
		} else {
			// 写入数据
			start := time.Now()
			select {
			case section := <-analogCh:
				wt := time.Now()
				if isRt {
					GlobalPlugin.WriteRtAnalog(unitNumber, section)
				} else {
					GlobalPlugin.WriteHisAnalog(unitNumber, section)
				}
				if isFast {
					FastAnalogWriteSectionInfoList = append(FastAnalogWriteSectionInfoList, WriteSectionInfo{
						UnitNumber:   unitNumber,
						Time:         section.Time,
						Duration:     time.Now().Sub(wt),
						SectionCount: 1,
						PNumCount:    int64(len(section.Data)),
					})
				} else {
					NormalAnalogWriteSectionInfoList = append(NormalAnalogWriteSectionInfoList, WriteSectionInfo{
						UnitNumber:   unitNumber,
						Time:         section.Time,
						Duration:     time.Now().Sub(wt),
						SectionCount: 1,
						PNumCount:    int64(len(section.Data)),
					})
				}
			}
			select {
			case section := <-digitalCh:
				wt := time.Now()
				if isRt {
					GlobalPlugin.WriteRtDigital(unitNumber, section)
				} else {
					GlobalPlugin.WriteHisDigital(unitNumber, section)
				}
				if isFast {
					FastDigitalWriteSectionInfoList = append(FastDigitalWriteSectionInfoList, WriteSectionInfo{
						UnitNumber:   unitNumber,
						Time:         section.Time,
						Duration:     time.Now().Sub(wt),
						SectionCount: 1,
						PNumCount:    int64(len(section.Data)),
					})
				} else {
					NormalDigitalWriteSectionInfoList = append(NormalDigitalWriteSectionInfoList, WriteSectionInfo{
						UnitNumber:   unitNumber,
						Time:         section.Time,
						Duration:     time.Now().Sub(wt),
						SectionCount: 1,
						PNumCount:    int64(len(section.Data)),
					})
				}
			}
			duration := time.Now().Sub(start)

			// 全部写完, 退出循环
			if len(closeChan) == 2 && len(analogCh) == 0 && len(digitalCh) == 0 {
				break
			}

			// 睡眠剩余时间
			if sum < overloadProtectionWriteDuration {
				sum += overloadProtectionWritePeriodic

				if duration < time.Duration(overloadProtectionWritePeriodic)*time.Millisecond {
					sleepDuration := time.Duration(overloadProtectionWritePeriodic)*time.Millisecond - duration
					if isFast {
						FastSleepDurationList = append(FastSleepDurationList, sleepDuration)
					} else {
						NormalSleepDurationList = append(NormalSleepDurationList, sleepDuration)
					}
					time.Sleep(sleepDuration)
				}
			} else {
				if duration < time.Duration(regularWritePeriodic)*time.Millisecond {
					sleepDuration := time.Duration(regularWritePeriodic)*time.Millisecond - duration
					if isFast {
						FastSleepDurationList = append(FastSleepDurationList, sleepDuration)
					} else {
						NormalSleepDurationList = append(NormalSleepDurationList, sleepDuration)
					}
					time.Sleep(sleepDuration)
				}
			}
		}
	}
	close(closeChan)
	close(analogCh)
	close(digitalCh)
}

// StaticWrite 静态写入
func StaticWrite(unitNumber int64, analogPath string, digitalPath string) {
	t1 := time.Now()
	analogSection := ReadStaticAnalogCsv(analogPath)
	GlobalPlugin.WriteStaticAnalog(unitNumber, analogSection)
	t2 := time.Now()
	digitalSection := ReadStaticDigitalCsv(digitalPath)
	GlobalPlugin.WriteStaticDigital(unitNumber, digitalSection)
	t3 := time.Now()
	FastAnalogWriteSectionInfoList = append(FastAnalogWriteSectionInfoList, WriteSectionInfo{
		UnitNumber:   unitNumber,
		Time:         -1,
		Duration:     t2.Sub(t1),
		SectionCount: int64(len(analogSection.Data)),
	})
	FastDigitalWriteSectionInfoList = append(FastDigitalWriteSectionInfoList, WriteSectionInfo{
		UnitNumber:   unitNumber,
		Time:         -1,
		Duration:     t3.Sub(t2),
		SectionCount: int64(len(digitalSection.Data)),
	})
	StaticSummary("静态写入", t1, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList)
}

// FastWriteRt 极速写入实时值
func FastWriteRt(unitNumber int64, fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	rd2 := make(chan bool, 1)
	rd3 := make(chan bool, 1)
	rd4 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		fmt.Println("捕获中断信号, 进行平滑退出处理")
		done <- true
		rd1 <- true
		rd2 <- true
		rd3 <- true
		rd4 <- true
		fmt.Println("平滑退出信号发送完成")
	}()

	closeCh := make(chan struct{}, 4)
	fastAnalogCh := make(chan AnalogSection, CacheSize)
	fastDigitalCh := make(chan DigitalSection, CacheSize)
	normalAnalogCh := make(chan AnalogSection, CacheSize)
	normalDigitalCh := make(chan DigitalSection, CacheSize)
	wg := new(sync.WaitGroup)
	wg.Add(4)
	go ReadAnalogCsv(wg, closeCh, fastAnalogCsvPath, fastAnalogCh, rd1)
	go ReadDigitalCsv(wg, closeCh, fastDigitalCsvPath, fastDigitalCh, rd2)
	go ReadAnalogCsv(wg, closeCh, normalAnalogCsvPath, normalAnalogCh, rd3)
	go ReadDigitalCsv(wg, closeCh, normalDigitalCsvPath, normalDigitalCh, rd4)
	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2 * time.Second)

	start := time.Now()
	FastWriteRealtimeSection(unitNumber, closeCh, fastAnalogCh, fastDigitalCh, normalAnalogCh, normalDigitalCh, done)
	wg.Wait()
	end := time.Now()

	RtFastWriteSummary("极速写入实时值", start, end, FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList)
}

// PeriodicWriteRt 周期性写入实时值
func PeriodicWriteRt(unitNumber int64, overloadProtectionFlag bool, fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string, fastCache bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done1 := make(chan bool, 1)
	done2 := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	rd2 := make(chan bool, 1)
	rd3 := make(chan bool, 1)
	rd4 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		done1 <- true
		done2 <- true
		rd1 <- true
		rd2 <- true
		rd3 <- true
		rd4 <- true
	}()

	fastCloseCh := make(chan struct{}, 2)
	normalCloseCh := make(chan struct{}, 2)
	fastAnalogCh := make(chan AnalogSection, CacheSize)
	fastDigitalCh := make(chan DigitalSection, CacheSize)
	normalAnalogCh := make(chan AnalogSection, CacheSize)
	normalDigitalCh := make(chan DigitalSection, CacheSize)
	wgRead := new(sync.WaitGroup)
	wgRead.Add(4)
	go ReadAnalogCsv(wgRead, fastCloseCh, fastAnalogCsvPath, fastAnalogCh, rd1)
	go ReadDigitalCsv(wgRead, fastCloseCh, fastDigitalCsvPath, fastDigitalCh, rd2)
	go ReadAnalogCsv(wgRead, normalCloseCh, normalAnalogCsvPath, normalAnalogCh, rd3)
	go ReadDigitalCsv(wgRead, normalCloseCh, normalDigitalCsvPath, normalDigitalCh, rd4)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2000 * time.Millisecond)
	start := time.Now()
	wgWrite := new(sync.WaitGroup)
	wgWrite.Add(2)
	if overloadProtectionFlag {
		go AsyncPeriodicWriteSection(unitNumber, wgWrite, 0, 0, FastRegularWritePeriodic, fastCloseCh, fastAnalogCh, fastDigitalCh, true, true, fastCache, done1)
		go AsyncPeriodicWriteSection(unitNumber, wgWrite, OverloadProtectionWriteDuration, OverloadProtectionWritePeriodic, NormalRegularWritePeriodic, normalCloseCh, normalAnalogCh, normalDigitalCh, true, false, false, done2)
	} else {
		go AsyncPeriodicWriteSection(unitNumber, wgWrite, 0, 0, FastRegularWritePeriodic, fastCloseCh, fastAnalogCh, fastDigitalCh, true, true, fastCache, done1)
		go AsyncPeriodicWriteSection(unitNumber, wgWrite, 0, 0, NormalRegularWritePeriodic, normalCloseCh, normalAnalogCh, normalDigitalCh, true, false, false, done2)
	}
	wgWrite.Wait()
	wgRead.Wait()
	end := time.Now()

	name := "周期性写入实时值(关闭过载保护)"
	if overloadProtectionFlag {
		name = "周期性写入实时值(开启载保护)"
	}
	PeriodicWriteRtSummary(name, start, end, FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, FastSleepDurationList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, NormalSleepDurationList)
}

// FastWriteHis 极速写历史
func FastWriteHis(unitNumber int64, analogCsvPath string, digitalCsvPath string) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	rd2 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		done <- true
		rd1 <- true
		rd2 <- true
	}()

	closeCh := make(chan struct{}, 4)
	analogCh := make(chan AnalogSection, CacheSize)
	digitalCh := make(chan DigitalSection, CacheSize)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go ReadAnalogCsv(wg, closeCh, analogCsvPath, analogCh, rd1)
	go ReadDigitalCsv(wg, closeCh, digitalCsvPath, digitalCh, rd2)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2000 * time.Millisecond)
	start := time.Now()
	FastWriteHisSection(unitNumber, closeCh, analogCh, digitalCh, done)
	wg.Wait()
	end := time.Now()

	HisFastWriteSummary("极速写入历史值", start, end, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList)
}

// PeriodicWriteHis 周期性写历史
func PeriodicWriteHis(unitNumber int64, analogCsvPath string, digitalCsvPath string) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	rd2 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		done <- true
		rd1 <- true
		rd2 <- true
	}()

	normalCloseCh := make(chan struct{}, 2)
	normalAnalogCh := make(chan AnalogSection, CacheSize)
	normalDigitalCh := make(chan DigitalSection, CacheSize)
	wgRead := new(sync.WaitGroup)
	wgRead.Add(2)
	go ReadAnalogCsv(wgRead, normalCloseCh, analogCsvPath, normalAnalogCh, rd1)
	go ReadDigitalCsv(wgRead, normalCloseCh, digitalCsvPath, normalDigitalCh, rd2)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2000 * time.Millisecond)

	start := time.Now()
	wgWrite := new(sync.WaitGroup)
	wgWrite.Add(1)
	AsyncPeriodicWriteSection(unitNumber, wgWrite, 0, 0, NormalRegularWritePeriodic, normalCloseCh, normalAnalogCh, normalDigitalCh, false, false, false, done)
	wgWrite.Wait()
	wgRead.Wait()
	end := time.Now()

	PeriodicWriteHisSummary("周期性写入历史值", start, end, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, NormalSleepDurationList)
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

func (df *WritePlugin) Login(param string) {
	if param == "" {
		C.dy_login(df.handle, nil)
	} else {
		cParam := C.CString(param)
		defer C.free(unsafe.Pointer(cParam))
		C.dy_login(df.handle, cParam)
	}
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

func (df *WritePlugin) WriteRtAnalogList(unitNumber int64, sections []AnalogSection) {
	if unitNumber == 1 {
		df.SyncWriteRtAnalogList(unitNumber, sections)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteRtAnalogList(wg, i, sections)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteRtDigitalList(unitNumber int64, sections []DigitalSection) {
	if unitNumber == 1 {
		df.SyncWriteRtDigitalList(unitNumber, sections)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteRtDigitalList(wg, i, sections)
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

func (df *WritePlugin) SyncWriteRtAnalogList(unitId int64, sections []AnalogSection) {
	// 初始化 C 数组
	timeList := make([]C.int64_t, 0)
	analogArrayList := make([]*C.Analog, 0)
	countList := make([]C.int64_t, 0)

	for i := range sections {
		timeList = append(timeList, C.int64_t(sections[i].Time))

		// 分配 C 内存并将 Go 数据复制到 C 内存中
		analogData := C.malloc(C.size_t(len(sections[i].Data)) * C.size_t(unsafe.Sizeof(C.Analog{})))
		if analogData == nil {
			panic("C.malloc failed")
		}
		for j := range sections[i].Data {
			(*[1 << 30]C.Analog)(analogData)[j] = C.Analog(sections[i].Data[j])
		}
		analogArrayList = append(analogArrayList, (*C.Analog)(analogData))
		countList = append(countList, C.int64_t(len(sections[i].Data)))
	}

	// 调用 C 函数，传递结构体指针数组
	C.dy_write_rt_analog_list(df.handle, C.int64_t(unitId), &timeList[0], &analogArrayList[0], &countList[0], C.int64_t(len(sections)))

	// 释放 C 分配的内存
	for i := range analogArrayList {
		if analogArrayList[i] != nil {
			C.free(unsafe.Pointer(analogArrayList[i]))
		}
	}
}

func (df *WritePlugin) SyncWriteRtDigitalList(unitId int64, sections []DigitalSection) {
	// 初始化 C 数组
	timeList := make([]C.int64_t, 0)
	digitalArrayList := make([]*C.Digital, 0)
	countList := make([]C.int64_t, 0)

	for i := range sections {
		timeList = append(timeList, C.int64_t(sections[i].Time))

		// 分配 C 内存并将 Go 数据复制到 C 内存中
		digitalData := C.malloc(C.size_t(len(sections[i].Data)) * C.size_t(unsafe.Sizeof(C.Digital{})))
		if digitalData == nil {
			panic("C.malloc failed")
		}
		for j := range sections[i].Data {
			(*[1 << 30]C.Digital)(digitalData)[j] = C.Digital(sections[i].Data[j])
		}
		digitalArrayList = append(digitalArrayList, (*C.Digital)(digitalData))
		countList = append(countList, C.int64_t(len(sections[i].Data)))
	}

	// 调用 C 函数，传递结构体指针数组
	C.dy_write_rt_digital_list(df.handle, C.int64_t(unitId), &timeList[0], &digitalArrayList[0], &countList[0], C.int64_t(len(sections)))

	// 释放 C 分配的内存
	for i := range digitalArrayList {
		if digitalArrayList[i] != nil {
			C.free(unsafe.Pointer(digitalArrayList[i]))
		}
	}
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

func (df *WritePlugin) AsyncWriteRtAnalogList(wg *sync.WaitGroup, unitId int64, sections []AnalogSection) {
	defer wg.Done()
	df.SyncWriteRtAnalogList(unitId, sections)
}

func (df *WritePlugin) AsyncWriteRtDigitalList(wg *sync.WaitGroup, unitId int64, sections []DigitalSection) {
	defer wg.Done()
	df.SyncWriteRtDigitalList(unitId, sections)
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
		param, _ := cmd.Flags().GetString("param")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login(param)

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
		param, _ := cmd.Flags().GetString("param")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login(param)

		// 极速写入实时值
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
		param, _ := cmd.Flags().GetString("param")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login(param)

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
		param, _ := cmd.Flags().GetString("param")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login(param)

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
		fastCache, _ := cmd.Flags().GetBool("fast_cache")
		param, _ := cmd.Flags().GetString("param")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		GlobalPlugin.Login(param)

		// 周期性写入
		PeriodicWriteRt(unitNumber, overloadProtection, fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath, fastCache)

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
	staticWrite.Flags().StringP("param", "", "", "custom param")

	rootCmd.AddCommand(rtFastWrite)
	rtFastWrite.Flags().StringP("plugin", "", "", "plugin path")
	rtFastWrite.Flags().StringP("rt_fast_analog", "", "", "realtime fast analog csv path")
	rtFastWrite.Flags().StringP("rt_fast_digital", "", "", "realtime fast digital csv path")
	rtFastWrite.Flags().StringP("rt_normal_analog", "", "", "realtime normal analog csv path")
	rtFastWrite.Flags().StringP("rt_normal_digital", "", "", "realtime normal digital csv path")
	rtFastWrite.Flags().Int64P("unit_number", "", 1, "unit number")
	rtFastWrite.Flags().StringP("param", "", "", "custom param")

	rootCmd.AddCommand(hisFastWrite)
	hisFastWrite.Flags().StringP("plugin", "", "", "plugin path")
	hisFastWrite.Flags().StringP("his_normal_analog", "", "", "history normal analog csv path")
	hisFastWrite.Flags().StringP("his_normal_digital", "", "", "history normal digital csv path")
	hisFastWrite.Flags().Int64P("unit_number", "", 1, "unit number")
	hisFastWrite.Flags().StringP("param", "", "", "custom param")

	rootCmd.AddCommand(hisPeriodicWrite)
	hisPeriodicWrite.Flags().StringP("plugin", "", "", "plugin path")
	hisPeriodicWrite.Flags().StringP("his_normal_analog", "", "", "history normal analog csv path")
	hisPeriodicWrite.Flags().StringP("his_normal_digital", "", "", "history normal digital csv path")
	hisPeriodicWrite.Flags().Int64P("unit_number", "", 1, "unit number")
	hisPeriodicWrite.Flags().StringP("param", "", "", "custom param")

	rootCmd.AddCommand(rtPeriodicWrite)
	rtPeriodicWrite.Flags().StringP("plugin", "", "", "plugin path")
	rtPeriodicWrite.Flags().BoolP("overload_protection", "", false, "overload protection flag")
	rtPeriodicWrite.Flags().StringP("rt_fast_analog", "", "", "realtime fast analog csv path")
	rtPeriodicWrite.Flags().StringP("rt_fast_digital", "", "", "realtime fast digital csv path")
	rtPeriodicWrite.Flags().StringP("rt_normal_analog", "", "", "realtime normal analog csv path")
	rtPeriodicWrite.Flags().StringP("rt_normal_digital", "", "", "realtime normal digital csv path")
	rtPeriodicWrite.Flags().Int64P("unit_number", "", 1, "unit number")
	rtPeriodicWrite.Flags().BoolP("fast_cache", "", false, "fast cache")
	rtPeriodicWrite.Flags().StringP("param", "", "", "custom param")
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

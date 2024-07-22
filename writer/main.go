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
	"math/rand"
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

func StaticSummary(magic int32, name string, start time.Time, end time.Time, analog []WriteSectionInfo, digital []WriteSectionInfo, logoutDuration time.Duration) {
	log.Printf("MAGIC: %v, %v - 开始时间: %v, 结束时间: %v\n", magic, name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	log.Printf("总耗时: %v, 机组数量: %v, 写入pnum数量: %v\n", analog[0].Duration+digital[0].Duration+logoutDuration, analog[0].UnitNumber, analog[0].PNumCount+digital[0].PNumCount)
}

func HisFastWriteSummary(
	magic int32, name string, start time.Time, end time.Time,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo,
	logoutDuration time.Duration,
) {
	log.Printf("MAGIC: %v, %v - 开始时间: %v, 结束时间: %v\n", magic, name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	if len(normalAnalog) != 0 && len(normalDigital) != 0 {
		nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)
		log.Printf("总耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v,\n\t\t最长耗时: %v, 最短耗时: %v, P99耗时: %v, P95耗时: %v, 中位数耗时: %v\n",
			nAll+logoutDuration, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
		)
	}
}

func ParallelRtFastWriteSummary(
	magic int32, name string, start time.Time, end time.Time,
	fastAnalog []WriteSectionInfo, fastDigital []WriteSectionInfo,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo,
	logoutDuration time.Duration,
) {
	log.Printf("MAGIC: %v, %v - 开始时间: %v, 结束时间: %v\n", magic, name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	allTime := time.Duration(0)
	if len(fastAnalog) != 0 && len(fastDigital) != 0 {
		fAll, fCount, fAvg, fMax, fMin, fP99, fP95, fP50, fPNum := Summary(fastAnalog, fastDigital)
		if allTime < fAll {
			allTime = fAll
		}
		log.Printf("快采点 - 总耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v, \n\t\t最长耗时: %v, 最短耗时: %v, P99耗时: %v, P95耗时: %v, 中位数耗时: %v\n",
			fAll, fCount, fPNum, fAvg, fMax, fMin, fP99, fP95, fP50,
		)
	}
	if len(normalAnalog) != 0 && len(normalDigital) != 0 {
		nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)
		if allTime < nAll {
			allTime = nAll
		}
		log.Printf("普通点 - 总耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v, \n\t\t最长耗时: %v, 最短耗时: %v, P99耗时: %v, P95耗时: %v, 中位数耗时: %v\n",
			nAll, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
		)
	}
	log.Printf("统计总耗时(刨除掉等待CSV读取时间): %v\n", allTime+logoutDuration)
	log.Printf("实际总耗时(会算上等待CSV读取时间): %v\n", end.Sub(start)+logoutDuration)
}

func RtFastWriteSummary(
	magic int32, name string, start time.Time, end time.Time,
	fastAnalog []WriteSectionInfo, fastDigital []WriteSectionInfo,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo,
	logoutDuration time.Duration,
) {
	log.Printf("MAGIC: %v, %v - 开始时间: %v, 结束时间: %v\n", magic, name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	all := time.Duration(0)
	if len(fastAnalog) != 0 && len(fastDigital) != 0 {
		fAll, fCount, fAvg, fMax, fMin, fP99, fP95, fP50, fPNum := Summary(fastAnalog, fastDigital)
		log.Printf("快采点 - 总耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v, \n\t\t最长耗时: %v, 最短耗时: %v, P99耗时: %v, P95耗时: %v, 中位数耗时: %v\n",
			fAll, fCount, fPNum, fAvg, fMax, fMin, fP99, fP95, fP50,
		)
		all += fAll
	}
	if len(normalAnalog) != 0 && len(normalDigital) != 0 {
		nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)
		log.Printf("普通点 - 总耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v, \n\t\t最长耗时: %v, 最短耗时: %v, P99耗时: %v, P95耗时: %v, 中位数耗时: %v\n",
			nAll, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
		)
		all += nAll
	}
	log.Printf("写入总耗时: %v\n", all+logoutDuration)
}

func PeriodicWriteHisSummary(
	magic int32, name string, start time.Time, end time.Time,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo, normalSleepList []time.Duration, logoutDuration time.Duration,
) {
	log.Printf("MAGIC: %v, %v - 开始时间: %v, 结束时间: %v\n", magic, name, start.Format(time.RFC3339), end.Format(time.RFC3339))
	if len(normalAnalog) != 0 && len(normalDigital) != 0 {
		nSleepSum := time.Duration(0)
		for _, d := range normalSleepList {
			nSleepSum += d
		}
		nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)
		log.Printf("总耗时: %v, 睡眠耗时: %v, 断面数量: %v, PNUM数量: %v, 平均耗时: %v, \n\t\t最长耗时: %v, 最短耗时: %v, P99耗时: %v, P95耗时: %v, 中位数耗时: %v\n",
			nAll+logoutDuration, nSleepSum, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
		)
	}
}

func PeriodicWriteRtSummary(
	magic int32, name string, start time.Time, end time.Time,
	fastAnalog []WriteSectionInfo, fastDigital []WriteSectionInfo, fastSleepList []time.Duration,
	normalAnalog []WriteSectionInfo, normalDigital []WriteSectionInfo, normalSleepList []time.Duration,
	logoutDuration time.Duration,
) {
	log.Printf("MAGIC: %v, %v - 开始时间: %v, 结束时间: %v\n", magic, name, start.Format(time.RFC3339), end.Format(time.RFC3339))

	if len(fastAnalog) != 0 && len(fastDigital) != 0 {
		fAll, fCount, fAvg, fMax, fMin, fP99, fP95, fP50, fPNum := Summary(fastAnalog, fastDigital)
		fSleepSum := time.Duration(0)
		for _, d := range fastSleepList {
			fSleepSum += d
		}
		log.Printf("快采点 - 总耗时: %v, 睡眠耗时: %v, 断面数量: %v, PNUM数量: %v, \n\t\t平均耗时: %v ,最长耗时: %v, 最短耗时: %v, P99耗时: %v, P95耗时: %v, 中位数耗时: %v\n",
			fAll+logoutDuration, fSleepSum, fCount, fPNum, fAvg, fMax, fMin, fP99, fP95, fP50,
		)
	}

	if len(normalAnalog) != 0 && len(normalDigital) != 0 {
		nSleepSum := time.Duration(0)
		for _, d := range normalSleepList {
			nSleepSum += d
		}
		nAll, nCount, nAvg, nMax, nMin, nP99, nP95, nP50, nPNum := Summary(normalAnalog, normalDigital)
		log.Printf("普通点 - 总耗时: %v, 睡眠耗时: %v, 断面数量: %v, PNUM数量: %v, \n\t\t平均耗时: %v ,最长耗时: %v, 最短耗时: %v, P99耗时: %v, P95耗时: %v, 中位数耗时: %v\n",
			nAll+logoutDuration, nSleepSum, nCount, nPNum, nAvg, nMax, nMin, nP99, nP95, nP50,
		)
	}
}

type Section struct {
	analogOk  bool
	analog    AnalogSection
	digitalOk bool
	digital   DigitalSection
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

func ReadCsv(wg2 *sync.WaitGroup, analogFilePath string, digitalFilePath string, sectionCh chan Section, exitCh chan bool) {
	defer wg2.Done()

	rd1 := make(chan bool, 1)
	rd2 := make(chan bool, 1)
	go func() {
		<-exitCh
		rd1 <- true
		rd2 <- true
		log.Println("ReadCsv 收到平滑退出信号")
	}()

	analogCh := make(chan AnalogSection, CacheSize)
	digitalCh := make(chan DigitalSection, CacheSize)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go ReadAnalogCsv(wg, analogFilePath, analogCh, rd1)
	go ReadDigitalCsv(wg, digitalFilePath, digitalCh, rd2)

	for {
		analogSection, ok1 := <-analogCh
		digitalSection, ok2 := <-digitalCh
		sectionCh <- Section{
			analogOk:  ok1,
			analog:    analogSection,
			digitalOk: ok2,
			digital:   digitalSection,
		}

		if !ok1 && !ok2 {
			break
		}
	}
	wg.Wait()
	log.Println("ReadCsv 平滑退出成功")
	close(sectionCh)
}

// ReadAnalogCsv 读取CSV文件, 将其转换成 C.Analog 结构后发送到缓存队列
func ReadAnalogCsv(wg *sync.WaitGroup, filepath string, ch chan AnalogSection, exitCh chan bool) {
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
		select {
		case <-exitCh:
			log.Println("信号中断CSV读取协程:", filepath)
			close(ch)
			return
		default:
			// 读取一行, 判断是否为EOF
			record, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					if len(dataList) != 0 {
						ch <- AnalogSection{Time: tsFlag, Data: dataList}
					}
					close(ch)
					return
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
}

// ReadDigitalCsv 读取CSV文件, 将其转换成 C.Digital 结构后发送到缓存队列
func ReadDigitalCsv(wg *sync.WaitGroup, filepath string, ch chan DigitalSection, exitCh chan bool) {
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
		select {
		case <-exitCh:
			log.Println("信号中断CSV读取协程:", filepath)
			close(ch)
			return
		default:
			// 读取一行, 判断是否为EOF
			record, err := reader.Read()
			if err != nil {
				if err.Error() == "EOF" {
					if len(dataList) != 0 {
						ch <- DigitalSection{Time: tsFlag, Data: dataList}
					}
					close(ch)
					return
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
func FastWriteRealtimeSection(magic int32, unitNumber int64, fastSectionCh chan Section, normalSectionCh chan Section, exitCh chan bool, randomAv bool) {
	fastClose := false
	normalClose := false
	for {
		select {
		case <-exitCh:
			if !fastClose {
				for {
					_, ok := <-fastSectionCh
					if !ok {
						break
					}
				}
			}
			if !normalClose {
				for {
					_, ok := <-normalSectionCh
					if !ok {
						break
					}
				}
			}
			return
		case section, ok := <-fastSectionCh:
			if !ok {
				fastClose = true
				if normalClose {
					return
				}
				continue
			}
			wt1 := time.Now()
			if section.analogOk {
				GlobalPlugin.WriteRtAnalog(magic, unitNumber, section.analog, true, randomAv)
			}
			wt2 := time.Now()
			if section.digitalOk {
				GlobalPlugin.WriteRtDigital(magic, unitNumber, section.digital, true)
			}
			wt3 := time.Now()

			FastAnalogWriteSectionInfoList = append(FastAnalogWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.analog.Time,
				Duration:     wt2.Sub(wt1),
				SectionCount: 1,
				PNumCount:    int64(len(section.analog.Data)),
			})
			FastDigitalWriteSectionInfoList = append(FastDigitalWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.digital.Time,
				Duration:     wt3.Sub(wt2),
				SectionCount: 1,
				PNumCount:    int64(len(section.digital.Data)),
			})
		case section, ok := <-normalSectionCh:
			if !ok {
				normalClose = true
				if fastClose {
					return
				}
				continue
			}
			wt1 := time.Now()
			if section.analogOk {
				GlobalPlugin.WriteRtAnalog(magic, unitNumber, section.analog, false, randomAv)
			}
			wt2 := time.Now()
			if section.digitalOk {
				GlobalPlugin.WriteRtDigital(magic, unitNumber, section.digital, false)
			}
			wt3 := time.Now()

			NormalAnalogWriteSectionInfoList = append(NormalAnalogWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.analog.Time,
				Duration:     wt2.Sub(wt1),
				SectionCount: 1,
				PNumCount:    int64(len(section.analog.Data)),
			})
			NormalDigitalWriteSectionInfoList = append(NormalDigitalWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.digital.Time,
				Duration:     wt3.Sub(wt2),
				SectionCount: 1,
				PNumCount:    int64(len(section.digital.Data)),
			})
		}
	}
}

// FastWriteHisSection 极速写入历史断面
func FastWriteHisSection(magic int32, unitNumber int64, sectionCh chan Section, exitCh chan bool, randomAv bool) {
	for {
		select {
		case <-exitCh:
			for {
				_, ok := <-sectionCh
				if !ok {
					return
				}
			}
		case section, ok := <-sectionCh:
			if !ok {
				return
			}
			wt1 := time.Now()
			if section.analogOk {
				GlobalPlugin.WriteHisAnalog(magic, unitNumber, section.analog, randomAv)
			}
			wt2 := time.Now()
			if section.digitalOk {
				GlobalPlugin.WriteHisDigital(magic, unitNumber, section.digital)
			}
			wt3 := time.Now()
			NormalAnalogWriteSectionInfoList = append(NormalAnalogWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.analog.Time,
				Duration:     wt2.Sub(wt1),
				SectionCount: 1,
				PNumCount:    int64(len(section.analog.Data)),
			})
			NormalDigitalWriteSectionInfoList = append(NormalDigitalWriteSectionInfoList, WriteSectionInfo{
				UnitNumber:   unitNumber,
				Time:         section.digital.Time,
				Duration:     wt3.Sub(wt2),
				SectionCount: 1,
				PNumCount:    int64(len(section.digital.Data)),
			})
		}
	}
}

// AsyncPeriodicWriteSection 周期性写入断面(实时/历史通用)
// unitNumber int64 机组数量
// overloadProtectionWriteDuration 过载保护持续时间, 单位毫秒
// overloadProtectionWritePeriodic 过载保护写入周期, 单位毫秒
// regularWritePeriodic 常规写入周期, 单位毫秒
// 返回值: 总时间, 写入时间, 睡眠时间
func AsyncPeriodicWriteSection(
	magic int32,
	unitNumber int64,
	wg *sync.WaitGroup,
	overloadProtectionWriteDuration int,
	overloadProtectionWritePeriodic int,
	regularWritePeriodic int,
	sectionCh chan Section,
	isRt bool,
	isFast bool,
	fastCache bool,
	exitCh chan bool,
	randomAv bool,
) {
	defer func() {
		wg.Done()
	}()

	sum := 0
	for {
		select {
		case <-exitCh:
			for {
				_, ok := <-sectionCh
				if !ok {
					return
				}
			}
		default:
			if fastCache {
				analogList := make([]AnalogSection, 0)
				digitalList := make([]DigitalSection, 0)
				isEOF := false
				for {
					section, ok := <-sectionCh
					if !ok {
						isEOF = true
						break
					}
					if section.analogOk {
						analogList = append(analogList, section.analog)
					}
					if section.digitalOk {
						digitalList = append(digitalList, section.digital)
					}
					if len(analogList) == 100 || len(digitalList) == 100 {
						break
					}
				}
				t1 := time.Now()
				GlobalPlugin.WriteRtAnalogList(magic, unitNumber, analogList, randomAv)
				t2 := time.Now()
				GlobalPlugin.WriteRtDigitalList(magic, unitNumber, digitalList)
				t3 := time.Now()
				duration := t3.Sub(t1)

				aPCount := 0
				for _, analog := range analogList {
					aPCount = aPCount + len(analog.Data)
				}
				dPCount := 0
				for _, digital := range digitalList {
					dPCount = dPCount + len(digital.Data)
				}
				FastAnalogWriteSectionInfoList = append(FastAnalogWriteSectionInfoList, WriteSectionInfo{
					UnitNumber:   unitNumber,
					Time:         analogList[0].Time,
					Duration:     t2.Sub(t1),
					SectionCount: int64(len(analogList)),
					PNumCount:    int64(aPCount),
				})
				FastDigitalWriteSectionInfoList = append(FastDigitalWriteSectionInfoList, WriteSectionInfo{
					UnitNumber:   unitNumber,
					Time:         analogList[0].Time,
					Duration:     t3.Sub(t2),
					SectionCount: int64(len(digitalList)),
					PNumCount:    int64(dPCount),
				})

				// 全部写完, 退出循环
				if isEOF {
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
				section, ok := <-sectionCh
				if !ok {
					break
				}
				if isRt {
					wt1 := time.Now()
					if section.analogOk {
						GlobalPlugin.WriteRtAnalog(magic, unitNumber, section.analog, isFast, randomAv)
					}
					wt2 := time.Now()
					if section.digitalOk {
						GlobalPlugin.WriteRtDigital(magic, unitNumber, section.digital, isFast)
					}
					wt3 := time.Now()
					if isFast {
						FastAnalogWriteSectionInfoList = append(FastAnalogWriteSectionInfoList, WriteSectionInfo{
							UnitNumber:   unitNumber,
							Time:         section.analog.Time,
							Duration:     wt2.Sub(wt1),
							SectionCount: 1,
							PNumCount:    int64(len(section.analog.Data)),
						})
						FastDigitalWriteSectionInfoList = append(FastDigitalWriteSectionInfoList, WriteSectionInfo{
							UnitNumber:   unitNumber,
							Time:         section.digital.Time,
							Duration:     wt3.Sub(wt2),
							SectionCount: 1,
							PNumCount:    int64(len(section.digital.Data)),
						})
					} else {
						NormalAnalogWriteSectionInfoList = append(NormalAnalogWriteSectionInfoList, WriteSectionInfo{
							UnitNumber:   unitNumber,
							Time:         section.analog.Time,
							Duration:     wt2.Sub(wt1),
							SectionCount: 1,
							PNumCount:    int64(len(section.analog.Data)),
						})
						NormalDigitalWriteSectionInfoList = append(NormalDigitalWriteSectionInfoList, WriteSectionInfo{
							UnitNumber:   unitNumber,
							Time:         section.digital.Time,
							Duration:     wt3.Sub(wt2),
							SectionCount: 1,
							PNumCount:    int64(len(section.digital.Data)),
						})
					}
				} else {
					wt1 := time.Now()
					if section.analogOk {
						GlobalPlugin.WriteHisAnalog(magic, unitNumber, section.analog, randomAv)
					}
					wt2 := time.Now()
					if section.digitalOk {
						GlobalPlugin.WriteHisDigital(magic, unitNumber, section.digital)
					}
					wt3 := time.Now()

					NormalAnalogWriteSectionInfoList = append(NormalAnalogWriteSectionInfoList, WriteSectionInfo{
						UnitNumber:   unitNumber,
						Time:         section.analog.Time,
						Duration:     wt2.Sub(wt1),
						SectionCount: 1,
						PNumCount:    int64(len(section.analog.Data)),
					})
					NormalDigitalWriteSectionInfoList = append(NormalDigitalWriteSectionInfoList, WriteSectionInfo{
						UnitNumber:   unitNumber,
						Time:         section.digital.Time,
						Duration:     wt3.Sub(wt2),
						SectionCount: 1,
						PNumCount:    int64(len(section.digital.Data)),
					})
				}

				duration := time.Now().Sub(start)

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
	}
}

// StaticWrite 静态写入
func StaticWrite(magic int32, unitNumber int64, analogPath string, digitalPath string, typ int64) {
	t1 := time.Now()
	analogSection := ReadStaticAnalogCsv(analogPath)
	GlobalPlugin.WriteStaticAnalog(magic, unitNumber, analogSection, typ)
	t2 := time.Now()
	digitalSection := ReadStaticDigitalCsv(digitalPath)
	GlobalPlugin.WriteStaticDigital(magic, unitNumber, digitalSection, typ)
	t3 := time.Now()
	FastAnalogWriteSectionInfoList = append(FastAnalogWriteSectionInfoList, WriteSectionInfo{
		UnitNumber:   unitNumber,
		Time:         -1,
		Duration:     t2.Sub(t1),
		SectionCount: 1,
		PNumCount:    int64(len(analogSection.Data)),
	})
	FastDigitalWriteSectionInfoList = append(FastDigitalWriteSectionInfoList, WriteSectionInfo{
		UnitNumber:   unitNumber,
		Time:         -1,
		Duration:     t3.Sub(t2),
		SectionCount: 1,
		PNumCount:    int64(len(digitalSection.Data)),
	})
}

func FastWriteRtOnlyFast(magic int32, unitNumber int64, fastAnalogCsvPath string, fastDigitalCsvPath string, randomAv bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		log.Println("捕获中断信号, 进行平滑退出处理")
		done <- true
		rd1 <- true
		log.Println("平滑退出信号发送完成")
	}()

	fastSectionCh := make(chan Section, CacheSize)
	normalSectionCh := make(chan Section, CacheSize)
	close(normalSectionCh)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go ReadCsv(wg, fastAnalogCsvPath, fastDigitalCsvPath, fastSectionCh, rd1)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2 * time.Second)

	FastWriteRealtimeSection(magic, unitNumber, fastSectionCh, normalSectionCh, done, randomAv)
	wg.Wait()
}

func FastWriteRtOnlyNormal(magic int32, unitNumber int64, normalAnalogCsvPath string, normalDigitalCsvPath string, randomAv bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		log.Println("捕获中断信号, 进行平滑退出处理")
		done <- true
		rd1 <- true
		log.Println("平滑退出信号发送完成")
	}()

	fastSectionCh := make(chan Section, CacheSize)
	close(fastSectionCh)
	normalSectionCh := make(chan Section, CacheSize)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go ReadCsv(wg, normalAnalogCsvPath, normalDigitalCsvPath, normalSectionCh, rd1)
	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2 * time.Second)

	FastWriteRealtimeSection(magic, unitNumber, fastSectionCh, normalSectionCh, done, randomAv)
	wg.Wait()
}

func ParallelFastWriteRt(magic int32, unitNumber int64, fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string, randomAv bool) {
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go func() {
		defer wg.Done()
		FastWriteRtOnlyFast(magic, unitNumber, fastAnalogCsvPath, fastDigitalCsvPath, randomAv)
	}()
	go func() {
		defer wg.Done()
		FastWriteRtOnlyNormal(magic, unitNumber, normalAnalogCsvPath, normalDigitalCsvPath, randomAv)
	}()
	wg.Wait()
}

// FastWriteRt 极速写入实时值
func FastWriteRt(magic int32, unitNumber int64, fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string, randomAv bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	rd2 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		log.Println("捕获中断信号, 进行平滑退出处理")
		done <- true
		rd1 <- true
		rd2 <- true
		log.Println("平滑退出信号发送完成")
	}()

	fastSectionCh := make(chan Section, CacheSize)
	normalSectionCh := make(chan Section, CacheSize)
	wg := new(sync.WaitGroup)
	wg.Add(2)
	go ReadCsv(wg, fastAnalogCsvPath, fastDigitalCsvPath, fastSectionCh, rd1)
	go ReadCsv(wg, normalAnalogCsvPath, normalDigitalCsvPath, normalSectionCh, rd2)
	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2 * time.Second)

	FastWriteRealtimeSection(magic, unitNumber, fastSectionCh, normalSectionCh, done, randomAv)
	wg.Wait()
}

func PeriodicWriteRtOnlyFast(magic int32, unitNumber int64, overloadProtectionFlag bool, fastAnalogCsvPath string, fastDigitalCsvPath string, fastCache bool, randomAv bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done1 := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		done1 <- true
		rd1 <- true
	}()

	fastSectionCh := make(chan Section, CacheSize)
	wgRead := new(sync.WaitGroup)
	wgRead.Add(1)
	go ReadCsv(wgRead, fastAnalogCsvPath, fastDigitalCsvPath, fastSectionCh, rd1)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2000 * time.Millisecond)
	wgWrite := new(sync.WaitGroup)
	wgWrite.Add(1)
	if overloadProtectionFlag {
		go AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, 0, 0, FastRegularWritePeriodic, fastSectionCh, true, true, fastCache, done1, randomAv)
	} else {
		go AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, 0, 0, FastRegularWritePeriodic, fastSectionCh, true, true, fastCache, done1, randomAv)
	}
	wgWrite.Wait()
	wgRead.Wait()
}

func PeriodicWriteRtOnlyNormal(magic int32, unitNumber int64, overloadProtectionFlag bool, normalAnalogCsvPath string, normalDigitalCsvPath string, fastCache bool, randomAv bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done1 := make(chan bool, 1)
	done2 := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		done1 <- true
		done2 <- true
		rd1 <- true
	}()

	normalSectionCh := make(chan Section, CacheSize)
	wgRead := new(sync.WaitGroup)
	wgRead.Add(1)
	go ReadCsv(wgRead, normalAnalogCsvPath, normalDigitalCsvPath, normalSectionCh, rd1)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2000 * time.Millisecond)
	wgWrite := new(sync.WaitGroup)
	wgWrite.Add(1)
	if overloadProtectionFlag {
		go AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, OverloadProtectionWriteDuration, OverloadProtectionWritePeriodic, NormalRegularWritePeriodic, normalSectionCh, true, false, false, done2, randomAv)
	} else {
		go AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, 0, 0, NormalRegularWritePeriodic, normalSectionCh, true, false, false, done2, randomAv)
	}
	wgWrite.Wait()
	wgRead.Wait()
}

// PeriodicWriteRt 周期性写入实时值
func PeriodicWriteRt(magic int32, unitNumber int64, overloadProtectionFlag bool, fastAnalogCsvPath string, fastDigitalCsvPath string, normalAnalogCsvPath string, normalDigitalCsvPath string, fastCache bool, randomAv bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done1 := make(chan bool, 1)
	done2 := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	rd2 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		done1 <- true
		done2 <- true
		rd1 <- true
		rd2 <- true
	}()

	fastSectionCh := make(chan Section, CacheSize)
	normalSectionCh := make(chan Section, CacheSize)
	wgRead := new(sync.WaitGroup)
	wgRead.Add(2)
	go ReadCsv(wgRead, fastAnalogCsvPath, fastDigitalCsvPath, fastSectionCh, rd1)
	go ReadCsv(wgRead, normalAnalogCsvPath, normalDigitalCsvPath, normalSectionCh, rd2)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2000 * time.Millisecond)
	wgWrite := new(sync.WaitGroup)
	wgWrite.Add(2)
	if overloadProtectionFlag {
		go AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, 0, 0, FastRegularWritePeriodic, fastSectionCh, true, true, fastCache, done1, randomAv)
		go AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, OverloadProtectionWriteDuration, OverloadProtectionWritePeriodic, NormalRegularWritePeriodic, normalSectionCh, true, false, false, done2, randomAv)
	} else {
		go AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, 0, 0, FastRegularWritePeriodic, fastSectionCh, true, true, fastCache, done1, randomAv)
		go AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, 0, 0, NormalRegularWritePeriodic, normalSectionCh, true, false, false, done2, randomAv)
	}
	wgWrite.Wait()
	wgRead.Wait()
}

// FastWriteHis 极速写历史
func FastWriteHis(magic int32, unitNumber int64, analogCsvPath string, digitalCsvPath string, randomAv bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		done <- true
		rd1 <- true
	}()

	sectionCh := make(chan Section, CacheSize)
	wg := new(sync.WaitGroup)
	wg.Add(1)
	go ReadCsv(wg, analogCsvPath, digitalCsvPath, sectionCh, rd1)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2000 * time.Millisecond)
	FastWriteHisSection(magic, unitNumber, sectionCh, done, randomAv)
	wg.Wait()
}

// PeriodicWriteHis 周期性写历史
func PeriodicWriteHis(magic int32, unitNumber int64, analogCsvPath string, digitalCsvPath string, randomAv bool) {
	// 平滑退出
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool, 1)
	rd1 := make(chan bool, 1)
	go func() {
		_ = <-sigs
		done <- true
		rd1 <- true
	}()

	normalSectionCh := make(chan Section, CacheSize)
	wgRead := new(sync.WaitGroup)
	wgRead.Add(1)
	go ReadCsv(wgRead, analogCsvPath, digitalCsvPath, normalSectionCh, rd1)

	// 睡眠2秒, 等待协程加载缓存
	time.Sleep(2000 * time.Millisecond)

	wgWrite := new(sync.WaitGroup)
	wgWrite.Add(1)
	AsyncPeriodicWriteSection(magic, unitNumber, wgWrite, 0, 0, NormalRegularWritePeriodic, normalSectionCh, false, false, false, done, randomAv)
	wgWrite.Wait()
	wgRead.Wait()
}

func RandAnalogSection(section AnalogSection) AnalogSection {
	ss := AnalogSection{
		Time: section.Time,
		Data: make([]C.Analog, 0),
	}
	for _, d := range section.Data {
		ss.Data = append(ss.Data, d)
	}
	for i := 0; i < len(ss.Data); i++ {
		ss.Data[i].av += C.float(float32(rand.Intn(30)))
	}
	return ss
}

// GlobalID 拼接GlobalID
// +-------+---------+-----------+---------+-------+-------+
// | 32bit |  8 bit  |   1bit    |  1 bit  | 1 bit | 21bit |
// +-------+---------+-----------+---------+-------+-------+
// | magic | unit_id | is_analog | is_fast | is_rt | p_num |
// +-------+---------+-----------+---------+-------+-------+
func GlobalID(magic int32, unitId int64, isAnalog bool, isFast bool, isRt bool, pNum int32) int64 {
	isAnalogVal := int64(0)
	if isAnalog {
		isAnalogVal = 1
	}
	isFastVal := int64(0)
	if isFast {
		isFastVal = 1
	}
	isRtVal := int64(0)
	if isRt {
		isRtVal = 1
	}
	return int64(magic)<<32 | unitId<<24 | isAnalogVal<<23 | isFastVal<<22 | isRtVal<<21 | int64(pNum)&0x1FFFFF
}

func InitAnalogGlobalID(magic int32, unitId int64, isFast bool, isRt bool, section AnalogSection) AnalogSection {
	for i := 0; i < len(section.Data); i++ {
		section.Data[i].global_id = C.int64_t(GlobalID(magic, unitId, true, isFast, isRt, int32(section.Data[i].p_num)))
	}
	return section
}

func InitDigitalGlobalID(magic int32, unitId int64, isFast bool, isRt bool, section DigitalSection) DigitalSection {
	for i := 0; i < len(section.Data); i++ {
		section.Data[i].global_id = C.int64_t(GlobalID(magic, unitId, false, isFast, isRt, int32(section.Data[i].p_num)))
	}
	return section
}

func InitStaticAnalogGlobalID(magic int32, unitId int64, isFast bool, isRt bool, section StaticAnalogSection) StaticAnalogSection {
	for i := 0; i < len(section.Data); i++ {
		section.Data[i].global_id = C.int64_t(GlobalID(magic, unitId, true, isFast, isRt, int32(section.Data[i].p_num)))
	}
	return section
}

func InitStaticDigitalGlobalID(magic int32, unitId int64, isFast bool, isRt bool, section StaticDigitalSection) StaticDigitalSection {
	for i := 0; i < len(section.Data); i++ {
		section.Data[i].global_id = C.int64_t(GlobalID(magic, unitId, false, isFast, isRt, int32(section.Data[i].p_num)))
	}
	return section
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

func (df *WritePlugin) Login(param string) int {
	if param == "" {
		return int(C.dy_login(df.handle, nil))
	} else {
		cParam := C.CString(param)
		defer C.free(unsafe.Pointer(cParam))
		return int(C.dy_login(df.handle, cParam))
	}
}

func (df *WritePlugin) Logout() {
	C.dy_logout(df.handle)
}

func (df *WritePlugin) WriteRtAnalog(magic int32, unitNumber int64, section AnalogSection, isFast bool, randomAv bool) {
	if unitNumber == 1 {
		df.SyncWriteRtAnalog(magic, 0, section, isFast, randomAv)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteRtAnalog(wg, magic, i, section, isFast, randomAv)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteRtDigital(magic int32, unitNumber int64, section DigitalSection, isFast bool) {
	if unitNumber == 1 {
		df.SyncWriteRtDigital(magic, 0, section, isFast)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteRtDigital(wg, magic, i, section, isFast)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteRtAnalogList(magic int32, unitNumber int64, sections []AnalogSection, randomAv bool) {
	if unitNumber == 1 {
		df.SyncWriteRtAnalogList(magic, 0, sections, randomAv)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteRtAnalogList(wg, magic, i, sections, randomAv)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteRtDigitalList(magic int32, unitNumber int64, sections []DigitalSection) {
	if unitNumber == 1 {
		df.SyncWriteRtDigitalList(magic, 0, sections)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteRtDigitalList(wg, magic, i, sections)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteHisAnalog(magic int32, unitNumber int64, section AnalogSection, randomAv bool) {
	if unitNumber == 1 {
		df.SyncWriteHisAnalog(magic, 0, section, randomAv)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteHisAnalog(wg, magic, i, section, randomAv)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteHisDigital(magic int32, unitNumber int64, section DigitalSection) {
	if unitNumber == 1 {
		df.SyncWriteHisDigital(magic, 0, section)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteHisDigital(wg, magic, i, section)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteStaticAnalog(magic int32, unitNumber int64, section StaticAnalogSection, typ int64) {
	if unitNumber == 1 {
		df.SyncWriteStaticAnalog(magic, 0, section, typ)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteStaticAnalog(wg, magic, i, section, typ)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) WriteStaticDigital(magic int32, unitNumber int64, section StaticDigitalSection, typ int64) {
	if unitNumber == 1 {
		df.SyncWriteStaticDigital(magic, 0, section, typ)
	} else {
		wg := new(sync.WaitGroup)
		wg.Add(int(unitNumber))
		for i := int64(0); i < unitNumber; i++ {
			go df.AsyncWriteStaticDigital(wg, magic, i, section, typ)
		}
		wg.Wait()
	}
}

func (df *WritePlugin) SyncWriteRtAnalog(magic int32, unitId int64, section AnalogSection, isFast bool, randomAv bool) {
	if randomAv {
		section = RandAnalogSection(section)
	}
	section = InitAnalogGlobalID(magic, unitId, isFast, true, section)
	C.dy_write_rt_analog(df.handle, C.int32_t(magic), C.int64_t(unitId), C.int64_t(section.Time), (*C.Analog)(&section.Data[0]), C.int64_t(len(section.Data)), C.bool(isFast))
}

func (df *WritePlugin) SyncWriteRtDigital(magic int32, unitId int64, section DigitalSection, isFast bool) {
	section = InitDigitalGlobalID(magic, unitId, isFast, true, section)
	C.dy_write_rt_digital(df.handle, C.int32_t(magic), C.int64_t(unitId), C.int64_t(section.Time), (*C.Digital)(&section.Data[0]), C.int64_t(len(section.Data)), C.bool(isFast))
}

func (df *WritePlugin) SyncWriteRtAnalogList(magic int32, unitId int64, sections []AnalogSection, randomAv bool) {
	if randomAv {
		for i := 0; i < len(sections); i++ {
			sections[i] = RandAnalogSection(sections[i])
		}
	}
	for i := 0; i < len(sections); i++ {
		sections[i] = InitAnalogGlobalID(magic, unitId, true, true, sections[i])
	}

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
	C.dy_write_rt_analog_list(df.handle, C.int32_t(magic), C.int64_t(unitId), &timeList[0], &analogArrayList[0], &countList[0], C.int64_t(len(sections)))

	// 释放 C 分配的内存
	for i := range analogArrayList {
		if analogArrayList[i] != nil {
			C.free(unsafe.Pointer(analogArrayList[i]))
		}
	}
}

func (df *WritePlugin) SyncWriteRtDigitalList(magic int32, unitId int64, sections []DigitalSection) {
	for i := 0; i < len(sections); i++ {
		sections[i] = InitDigitalGlobalID(magic, unitId, true, true, sections[i])
	}

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
	C.dy_write_rt_digital_list(df.handle, C.int32_t(magic), C.int64_t(unitId), &timeList[0], &digitalArrayList[0], &countList[0], C.int64_t(len(sections)))

	// 释放 C 分配的内存
	for i := range digitalArrayList {
		if digitalArrayList[i] != nil {
			C.free(unsafe.Pointer(digitalArrayList[i]))
		}
	}
}

func (df *WritePlugin) SyncWriteHisAnalog(magic int32, unitId int64, section AnalogSection, randomAv bool) {
	if randomAv {
		section = RandAnalogSection(section)
	}
	section = InitAnalogGlobalID(magic, unitId, false, false, section)
	C.dy_write_his_analog(df.handle, C.int32_t(magic), C.int64_t(unitId), C.int64_t(section.Time), (*C.Analog)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) SyncWriteHisDigital(magic int32, unitId int64, section DigitalSection) {
	section = InitDigitalGlobalID(magic, unitId, false, false, section)
	C.dy_write_his_digital(df.handle, C.int32_t(magic), C.int64_t(unitId), C.int64_t(section.Time), (*C.Digital)(&section.Data[0]), C.int64_t(len(section.Data)))
}

func (df *WritePlugin) SyncWriteStaticAnalog(magic int32, unitId int64, section StaticAnalogSection, typ int64) {
	if typ == 0 {
		section = InitStaticAnalogGlobalID(magic, unitId, true, true, section)
	} else if typ == 1 {
		section = InitStaticAnalogGlobalID(magic, unitId, false, true, section)
	} else if typ == 2 {
		section = InitStaticAnalogGlobalID(magic, unitId, false, false, section)
	} else {
		panic("未知type: 0代表实时快采集点, 1代表实时普通点, 2代表历史普通点")
	}
	C.dy_write_static_analog(df.handle, C.int32_t(magic), C.int64_t(unitId), (*C.StaticAnalog)(&section.Data[0]), C.int64_t(len(section.Data)), C.int64_t(typ))
}

func (df *WritePlugin) SyncWriteStaticDigital(magic int32, unitId int64, section StaticDigitalSection, typ int64) {
	if typ == 0 {
		section = InitStaticDigitalGlobalID(magic, unitId, true, true, section)
	} else if typ == 1 {
		section = InitStaticDigitalGlobalID(magic, unitId, false, true, section)
	} else if typ == 2 {
		section = InitStaticDigitalGlobalID(magic, unitId, false, false, section)
	} else {
		panic("未知type: 0代表实时快采集点, 1代表实时普通点, 2代表历史普通点")
	}
	C.dy_write_static_digital(df.handle, C.int32_t(magic), C.int64_t(unitId), (*C.StaticDigital)(&section.Data[0]), C.int64_t(len(section.Data)), C.int64_t(typ))
}

func (df *WritePlugin) AsyncWriteRtAnalog(wg *sync.WaitGroup, magic int32, unitId int64, section AnalogSection, isFast bool, randomAv bool) {
	defer wg.Done()
	df.SyncWriteRtAnalog(magic, unitId, section, isFast, randomAv)
}

func (df *WritePlugin) AsyncWriteRtDigital(wg *sync.WaitGroup, magic int32, unitId int64, section DigitalSection, isFast bool) {
	defer wg.Done()
	df.SyncWriteRtDigital(magic, unitId, section, isFast)
}

func (df *WritePlugin) AsyncWriteRtAnalogList(wg *sync.WaitGroup, magic int32, unitId int64, sections []AnalogSection, randomAv bool) {
	defer wg.Done()
	df.SyncWriteRtAnalogList(magic, unitId, sections, randomAv)
}

func (df *WritePlugin) AsyncWriteRtDigitalList(wg *sync.WaitGroup, magic int32, unitId int64, sections []DigitalSection) {
	defer wg.Done()
	df.SyncWriteRtDigitalList(magic, unitId, sections)
}

func (df *WritePlugin) AsyncWriteHisAnalog(wg *sync.WaitGroup, magic int32, unitId int64, section AnalogSection, randomAv bool) {
	defer wg.Done()
	df.SyncWriteHisAnalog(magic, unitId, section, randomAv)
}

func (df *WritePlugin) AsyncWriteHisDigital(wg *sync.WaitGroup, magic int32, unitId int64, section DigitalSection) {
	defer wg.Done()
	df.SyncWriteHisDigital(magic, unitId, section)
}

func (df *WritePlugin) AsyncWriteStaticAnalog(wg *sync.WaitGroup, magic int32, unitId int64, section StaticAnalogSection, typ int64) {
	defer wg.Done()
	df.SyncWriteStaticAnalog(magic, unitId, section, typ)
}

func (df *WritePlugin) AsyncWriteStaticDigital(wg *sync.WaitGroup, magic int32, unitId int64, section StaticDigitalSection, typ int64) {
	defer wg.Done()
	df.SyncWriteStaticDigital(magic, unitId, section, typ)
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
		fmt.Println("v1.0.1")
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
		typ, _ := cmd.Flags().GetInt64("type")
		param, _ := cmd.Flags().GetString("param")
		magic, _ := cmd.Flags().GetInt32("magic")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		if rtn := GlobalPlugin.Login(param); rtn != 0 {
			log.Println("登陆失败: ", rtn)
			return
		}
		start := time.Now()

		// 输出统计值
		defer func() {
			logoutStart := time.Now()
			GlobalPlugin.Logout()
			logoutDuration := time.Since(logoutStart)

			log.Println("logout time: ", logoutDuration)
			StaticSummary(magic, "静态写入", start, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, logoutDuration)
		}()

		// 静态写入
		StaticWrite(magic, unitNumber, staticAnalogCsvPath, staticDigitalCsvPath, typ)
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
		randomAv, _ := cmd.Flags().GetBool("random_av")
		param, _ := cmd.Flags().GetString("param")
		mode, _ := cmd.Flags().GetInt64("mode")
		magic, _ := cmd.Flags().GetInt32("magic")
		parallelWriting, _ := cmd.Flags().GetBool("parallel_writing")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		if rtn := GlobalPlugin.Login(param); rtn != 0 {
			log.Println("登陆失败: ", rtn)
			return
		}
		start := time.Now()
		defer func() {
			logoutStart := time.Now()
			GlobalPlugin.Logout()
			logoutDuration := time.Since(logoutStart)
			log.Println("logout time: ", logoutDuration)
			if mode == 0 {
				if parallelWriting {
					ParallelRtFastWriteSummary(magic, "极速写入实时值(块采点,普通点并行)", start, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, logoutDuration)
				} else {
					RtFastWriteSummary(magic, "极速写入实时值(快采点,普通点串行)", start, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, logoutDuration)
				}
			} else if mode == 1 {
				RtFastWriteSummary(magic, "极速写入实时值(只写快采点)", start, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, logoutDuration)
			} else if mode == 2 {
				RtFastWriteSummary(magic, "极速写入实时值(只写普通点)", start, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, logoutDuration)
			} else {
				panic("mode must be 0 or 1 or 2")
			}
		}()

		// 极速写入实时值
		if mode == 0 {
			// 写快采 + 普通
			if parallelWriting {
				ParallelFastWriteRt(magic, unitNumber, fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath, randomAv)
			} else {
				FastWriteRt(magic, unitNumber, fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath, randomAv)
			}
		} else if mode == 1 {
			// 只写快采
			FastWriteRtOnlyFast(magic, unitNumber, fastAnalogCsvPath, fastDigitalCsvPath, randomAv)
		} else if mode == 2 {
			// 只写普通
			FastWriteRtOnlyNormal(magic, unitNumber, normalAnalogCsvPath, normalDigitalCsvPath, randomAv)
		} else {
			panic("mode must be 0 or 1 or 2")
		}
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
		randomAv, _ := cmd.Flags().GetBool("random_av")
		param, _ := cmd.Flags().GetString("param")
		magic, _ := cmd.Flags().GetInt32("magic")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		if rtn := GlobalPlugin.Login(param); rtn != 0 {
			log.Println("登陆失败: ", rtn)
			return
		}
		start := time.Now()
		defer func() {
			logoutStart := time.Now()
			GlobalPlugin.Logout()
			logoutDuration := time.Since(logoutStart)
			log.Println("logout time: ", logoutDuration)
			HisFastWriteSummary(magic, "极速写入历史值", start, time.Now(), NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, logoutDuration)
		}()

		// 极速写入历史
		FastWriteHis(magic, unitNumber, analogCsvPath, digitalCsvPath, randomAv)
	},
}

var hisPeriodicWrite = &cobra.Command{
	Use:   "his_periodic_write",
	Short: "Periodic Write HISTORY_NORMAL_ANALOG.csv, HISTORY_NORMAL_DIGITAL.csv",
	Run: func(cmd *cobra.Command, args []string) {
		pluginPath, _ := cmd.Flags().GetString("plugin")
		analogCsvPath, _ := cmd.Flags().GetString("his_normal_analog")
		digitalCsvPath, _ := cmd.Flags().GetString("his_normal_digital")
		randomAv, _ := cmd.Flags().GetBool("random_av")
		unitNumber, _ := cmd.Flags().GetInt64("unit_number")
		param, _ := cmd.Flags().GetString("param")
		magic, _ := cmd.Flags().GetInt32("magic")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		if rtn := GlobalPlugin.Login(param); rtn != 0 {
			log.Println("登陆失败: ", rtn)
			return
		}
		start := time.Now()
		defer func() {
			logoutStart := time.Now()
			GlobalPlugin.Logout()
			logoutDuration := time.Since(logoutStart)
			log.Println("logout time: ", logoutDuration)
			PeriodicWriteHisSummary(magic, "周期性写入历史值", start, time.Now(), NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, NormalSleepDurationList, logoutDuration)
		}()

		// 周期性写入
		PeriodicWriteHis(magic, unitNumber, analogCsvPath, digitalCsvPath, randomAv)
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
		randomAv, _ := cmd.Flags().GetBool("random_av")
		param, _ := cmd.Flags().GetString("param")
		mode, _ := cmd.Flags().GetInt64("mode")
		magic, _ := cmd.Flags().GetInt32("magic")

		// 加载动态库
		InitGlobalPlugin(pluginPath)

		// 登入
		if rtn := GlobalPlugin.Login(param); rtn != 0 {
			log.Println("登陆失败: ", rtn)
			return
		}
		start := time.Now()
		defer func() {
			logoutStart := time.Now()
			GlobalPlugin.Logout()
			logoutDuration := time.Since(logoutStart)
			log.Println("logout time: ", logoutDuration)

			name := ""
			if overloadProtection == true && fastCache == true {
				name = "周期性写入实时值(开启载保护, 开启快采点缓存)"
			} else if overloadProtection == true && fastCache == false {
				name = "周期性写入实时值(开启载保护, 关闭快采点缓存)"
			} else if overloadProtection == false && fastCache == false {
				name = "周期性写入实时值(关闭载保护, 关闭快采点缓存)"
			} else if overloadProtection == false && fastCache == true {
				name = "周期性写入实时值(关闭载保护, 开启快采点缓存)"
			}

			if mode == 0 {
				PeriodicWriteRtSummary(magic, name, start, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, FastSleepDurationList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, NormalSleepDurationList, logoutDuration)
			} else if mode == 1 {
				PeriodicWriteRtSummary(magic, name, start, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, FastSleepDurationList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, NormalSleepDurationList, logoutDuration)
			} else if mode == 2 {
				PeriodicWriteRtSummary(magic, name, start, time.Now(), FastAnalogWriteSectionInfoList, FastDigitalWriteSectionInfoList, FastSleepDurationList, NormalAnalogWriteSectionInfoList, NormalDigitalWriteSectionInfoList, NormalSleepDurationList, logoutDuration)
			} else {
				panic("mode must be 0 or 1 or 2")
			}
		}()

		// 周期性写入
		if mode == 0 {
			PeriodicWriteRt(magic, unitNumber, overloadProtection, fastAnalogCsvPath, fastDigitalCsvPath, normalAnalogCsvPath, normalDigitalCsvPath, fastCache, randomAv)
		} else if mode == 1 {
			PeriodicWriteRtOnlyFast(magic, unitNumber, overloadProtection, fastAnalogCsvPath, fastDigitalCsvPath, fastCache, randomAv)
		} else if mode == 2 {
			PeriodicWriteRtOnlyNormal(magic, unitNumber, overloadProtection, normalAnalogCsvPath, normalDigitalCsvPath, fastCache, randomAv)
		} else {
			panic("mode must be 0 or 1 or 2")
		}
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
	staticWrite.Flags().Int64P("type", "", 0, "0代表实时快采集点, 1代表实时普通点, 2代表历史普通点")
	staticWrite.Flags().StringP("param", "", "", "custom param")
	staticWrite.Flags().Int32P("magic", "", 0, "魔数, 默认为0")

	rootCmd.AddCommand(rtFastWrite)
	rtFastWrite.Flags().StringP("plugin", "", "", "plugin path")
	rtFastWrite.Flags().StringP("rt_fast_analog", "", "", "realtime fast analog csv path")
	rtFastWrite.Flags().StringP("rt_fast_digital", "", "", "realtime fast digital csv path")
	rtFastWrite.Flags().StringP("rt_normal_analog", "", "", "realtime normal analog csv path")
	rtFastWrite.Flags().StringP("rt_normal_digital", "", "", "realtime normal digital csv path")
	rtFastWrite.Flags().Int64P("unit_number", "", 1, "unit number")
	rtFastWrite.Flags().StringP("param", "", "", "custom param")
	rtFastWrite.Flags().BoolP("random_av", "", false, "为true表示给av值加一个[0,30]的随机数浮动")
	rtFastWrite.Flags().Int32P("magic", "", 0, "魔数, 默认为0")
	rtFastWrite.Flags().Int64("mode", 0, "写入模式: 0表示写快采点+普通点, 1表示只写快采点, 2表示只写普通点")
	rtFastWrite.Flags().BoolP("parallel_writing", "", false, "为true时, 块采点和普通点会分别由两个协程进行并行写入")

	rootCmd.AddCommand(rtPeriodicWrite)
	rtPeriodicWrite.Flags().StringP("plugin", "", "", "plugin path")
	rtPeriodicWrite.Flags().BoolP("overload_protection", "", false, "overload protection flag")
	rtPeriodicWrite.Flags().StringP("rt_fast_analog", "", "", "realtime fast analog csv path")
	rtPeriodicWrite.Flags().StringP("rt_fast_digital", "", "", "realtime fast digital csv path")
	rtPeriodicWrite.Flags().StringP("rt_normal_analog", "", "", "realtime normal analog csv path")
	rtPeriodicWrite.Flags().StringP("rt_normal_digital", "", "", "realtime normal digital csv path")
	rtPeriodicWrite.Flags().Int64P("unit_number", "", 1, "unit number")
	rtPeriodicWrite.Flags().BoolP("fast_cache", "", false, "fast cache")
	rtPeriodicWrite.Flags().BoolP("random_av", "", false, "为true表示给av值加一个[0,30]的随机数浮动")
	rtPeriodicWrite.Flags().StringP("param", "", "", "custom param")
	rtPeriodicWrite.Flags().Int32P("magic", "", 0, "魔数, 默认为0")
	rtPeriodicWrite.Flags().Int64("mode", 0, "写入模式: 0表示写快采点+普通点, 1表示只写快采点, 2表示只写普通点")

	rootCmd.AddCommand(hisFastWrite)
	hisFastWrite.Flags().StringP("plugin", "", "", "plugin path")
	hisFastWrite.Flags().StringP("his_normal_analog", "", "", "history normal analog csv path")
	hisFastWrite.Flags().StringP("his_normal_digital", "", "", "history normal digital csv path")
	hisFastWrite.Flags().Int64P("unit_number", "", 1, "unit number")
	hisFastWrite.Flags().BoolP("random_av", "", false, "为true表示给av值加一个[0,30]的随机数浮动")
	hisFastWrite.Flags().Int32P("magic", "", 0, "魔数, 默认为0")
	hisFastWrite.Flags().StringP("param", "", "", "custom param")

	rootCmd.AddCommand(hisPeriodicWrite)
	hisPeriodicWrite.Flags().StringP("plugin", "", "", "plugin path")
	hisPeriodicWrite.Flags().StringP("his_normal_analog", "", "", "history normal analog csv path")
	hisPeriodicWrite.Flags().StringP("his_normal_digital", "", "", "history normal digital csv path")
	hisPeriodicWrite.Flags().Int64P("unit_number", "", 1, "unit number")
	hisPeriodicWrite.Flags().BoolP("random_av", "", false, "为true表示给av值加一个[0,30]的随机数浮动")
	hisPeriodicWrite.Flags().Int32P("magic", "", 0, "魔数, 默认为0")
	hisPeriodicWrite.Flags().StringP("param", "", "", "custom param")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func main() {
	Execute()
}

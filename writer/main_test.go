package main

import "testing"

func TestReadAnalogCsv(t *testing.T) {
	ReadAnalogCsv("../CSV20240614/1718350759143_REALTIME_FAST_ANALOG.csv", nil, nil)
}

func TestReadDigitalCsv(t *testing.T) {
	ReadDigitalCsv("../CSV20240614/1718350759143_REALTIME_FAST_DIGITAL.csv", nil, nil)
}

func TestReadStaticAnalogCsv(t *testing.T) {
	ReadStaticAnalogCsv("../CSV20240614/1718350759143_REALTIME_FAST_STATIC_ANALOG.csv")
}

func TestReadStaticDigitalCsv(t *testing.T) {
	ReadStaticDigitalCsv("../CSV20240614/1718350759143_REALTIME_FAST_STATIC_DIGITAL.csv")
}

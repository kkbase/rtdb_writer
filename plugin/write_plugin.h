#ifndef _WRITE_PLUGIN_H_
#define _WRITE_PLUGIN_H_

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// 模拟量结构
typedef struct _Analog_ {
    int32_t p_num;  // P_NUM, 4Byte
    float av;       // AV, 4Byte
    float avr;      // AVR, 4Byte
    bool q;         // Q, 1Byte
    bool bf;        // BF, 1Byte
    bool qf;        // QF, 1Byte
    float fai;      // FAI, 4Byte
    bool ms;        // MS, 1Byte
    char tew;       // TEW, 1Byte
    uint16_t cst;   // CST, 2Byte
} Analog;

// 数字量结构
typedef struct _Digital_ {
    int32_t p_num;       // P_NUM, 4Byte
    bool dv;            // DV, 1Byte
    bool dvr;           // DVR, 1Byte
    bool q;             // Q, 1Byte
    bool bf;            // BF, 1Byte
    bool bq;            // FQ, 1Byte
    bool fai;           // FAI, 1Byte
    bool ms;            // MS, 1Byte
    char tew;           // TEW, 1Byte
    uint16_t cst;       // CST, 2Byte
} Digital;

// 静态模拟量结构
typedef struct _StaticAnalog_ {
    int32_t p_num;      // P_NUM, 4Byte
    uint16_t tagt;      // TAGT, 1Byte
    uint16_t fack;      // FACK, 1Byte
    bool l4ar;          // L4AR, 1Byte
    bool l3ar;          // L3AR, 1Byte
    bool l2ar;          // L2AR, 1Byte
    bool l1ar;          // L1AR, 1Byte
    bool h4ar;          // H4AR, 1Byte
    bool h3ar;          // H3AR, 1Byte
    bool h2ar;          // H2AR, 1Byte
    bool h1ar;          // H1AR, 1Byte
    char chn[32];       // CHN, 32Byte
    char pn[32];        // PN, 32Byte
    char desc[128];     // DESC, 128Byte
    char unit[32];      // UNIT, 32Byte
    float mu;           // MU, 4Byte
    float md;           // MD, 4Byte
} StaticAnalog;

// 静态数字量结构
typedef struct _StaticDigital_ {
    int32_t p_num;      // P_NUM, 4Byte
    uint16_t fack;      // FACK, 2Byte
    char chn[32];       // CHN, 32Byte
    char pn[32];        // PN, 32Byte
    char desc[128];     // DESC, 128Byte
    char unit[32];      // UNIT, 32Byte
} StaticDigital;

// 登陆数据库
void login();

// 登出数据库
void logout();

// 写实时模拟量
// unit_id: 机组ID
// time: 断面时间戳
// analog_array_ptr: 指向模拟量数组的指针
// count: 数组长度
void write_rt_analog(int64_t unit_id, int64_t time, Analog *analog_array_ptr, int64_t count);

// 写实时数字量
// unit_id: 机组ID
// time: 断面时间戳
// digital_array_ptr: 指向数字量数组的指针
// count: 数组长度
void write_rt_digital(int64_t unit_id, int64_t time, Digital *digital_array_ptr, int64_t count);

// 写历史模拟量
// unit_id: 机组ID
// time: 断面时间戳
// analog_array_ptr: 指向模拟量数组的指针
// count: 数组长度
void write_his_analog(int64_t unit_id, int64_t time, Analog *analog_array_ptr, int64_t count);

// 写历史数字量
// unit_id: 机组ID
// time: 断面时间戳
// digital_array_ptr: 指向数字量数组的指针
// count: 数组长度
void write_his_digital(int64_t unit_id, int64_t time, Digital *digital_array_ptr, int64_t count);

// 写静态模拟量
// unit_id: 机组ID
// static_analog_array_ptr: 指向静态模拟量数组的指针
// count: 数组长度
void write_static_analog(int64_t unit_id, StaticAnalog *static_analog_array_ptr, int64_t count);

// 写静态数字量
// unit_id: 机组ID
// static_digital_array_ptr: 指向静态数字量数组的指针
// count: 数组长度
void write_static_digital(int64_t unit_id, StaticDigital *static_digital_array_ptr, int64_t count);

#ifdef __cplusplus
}
#endif

#endif // _WRITE_PLUGIN_H_

#ifndef _WRITE_PLUGIN_H_
#define _WRITE_PLUGIN_H_

#include <stdint.h>
#include <stdbool.h>

//
// global_id是一个全局唯一的ID, 格式如下:
// +-------+---------+-----------+---------+-------+-------+
// | 32bit |  8 bit  |   1bit    |  1 bit  | 1 bit | 21bit |
// +-------+---------+-----------+---------+-------+-------+
// | magic | unit_id | is_analog | is_fast | is_rt | p_num |
// +-------+---------+-----------+---------+-------+-------+
//
// * magic: 魔数, 由用户手动输入(默认为0)
// * unit_id: 机组ID
// * is_analog: 1表示模拟量, 0表示数字量
// * is_fast: 1表示块采点, 0表示普通点
// * p_num: 对应CSV中的PNUM
//

#ifdef __cplusplus
extern "C" {
#endif

// 模拟量结构
typedef struct _Analog_ {
    int64_t global_id; // 全局ID
    int32_t p_num;     // P_NUM, 4Byte
    float av;          // AV, 4Byte
    float avr;         // AVR, 4Byte
    bool q;            // Q, 1Byte
    bool bf;           // BF, 1Byte
    bool qf;           // QF, 1Byte
    float fai;         // FAI, 4Byte
    bool ms;           // MS, 1Byte
    char tew;          // TEW, 1Byte
    uint16_t cst;      // CST, 2Byte
} Analog;

// 数字量结构
typedef struct _Digital_ {
    int64_t global_id;  // 全局ID
    int32_t p_num;      // P_NUM, 4Byte
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
    int64_t global_id;  // 全局ID
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
    int64_t global_id;  // 全局ID
    int32_t p_num;      // P_NUM, 4Byte
    uint16_t fack;      // FACK, 2Byte
    char chn[32];       // CHN, 32Byte
    char pn[32];        // PN, 32Byte
    char desc[128];     // DESC, 128Byte
    char unit[32];      // UNIT, 32Byte
} StaticDigital;

// 登陆数据库
// param是命令行向login传递的参数, 如果参数为空则param为NULL
int login(char *param);

// 登出数据库
void logout();

// 写实时模拟量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// time: 断面时间戳
// analog_array_ptr: 指向模拟量数组的指针
// count: 数组长度
// is_fast: 当为true时表示写快采点, 当为false时表示写普通点
void write_rt_analog(int32_t magic, int64_t unit_id, int64_t time, Analog *analog_array_ptr, int64_t count, bool is_fast);

// 写实时数字量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// time: 断面时间戳
// digital_array_ptr: 指向数字量数组的指针
// count: 数组长度
// is_fast: 当为true时表示写快采点, 当为false时表示写普通点
void write_rt_digital(int32_t magic, int64_t unit_id, int64_t time, Digital *digital_array_ptr, int64_t count, bool is_fast);

// 批量写实时模拟量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// count: 断面数量
// time: 时间列表, 包含count个时间
// analog_array_array_ptr: 模拟量断面数组, 包含count个断面的模拟量
// array_count: 每个断面中包含值的数量
// 备注: 只有写快采点的时候会调用此接口
void write_rt_analog_list(int32_t magic, int64_t unit_id, int64_t *time, Analog **analog_array_array_ptr, int64_t *array_count, int64_t count);

// 批量写实时数字量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// count: 断面数量
// time: 时间列表, 包含count个时间
// analog_array_array_ptr: 数字量断面数组, 包含count个断面的数字量
// array_count: 每个断面中包含值的数量
// 备注: 只有写快采点的时候会调用此接口
void write_rt_digital_list(int32_t magic, int64_t unit_id, int64_t *time, Digital **digital_array_array_ptr, int64_t *array_count, int64_t count);

// 写历史模拟量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// time: 断面时间戳
// analog_array_ptr: 指向模拟量数组的指针
// count: 数组长度
void write_his_analog(int32_t magic, int64_t unit_id, int64_t time, Analog *analog_array_ptr, int64_t count);

// 写历史数字量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// time: 断面时间戳
// digital_array_ptr: 指向数字量数组的指针
// count: 数组长度
void write_his_digital(int32_t magic, int64_t unit_id, int64_t time, Digital *digital_array_ptr, int64_t count);

// 写静态模拟量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// static_analog_array_ptr: 指向静态模拟量数组的指针
// count: 数组长度
// type: 数据类型, 通过命令行传递, 0代表实时快采集点, 1代表实时普通点, 2代表历史普通点
void write_static_analog(int32_t magic, int64_t unit_id, StaticAnalog *static_analog_array_ptr, int64_t count, int64_t type);

// 写静态数字量
// magic: 魔数, 用于标记测试数据集
// unit_id: 机组ID
// static_digital_array_ptr: 指向静态数字量数组的指针
// count: 数组长度
// type: 数据类型, 通过命令行传递, 0代表实时快采集点, 1代表实时普通点, 2代表历史普通点
void write_static_digital(int32_t magic, int64_t unit_id, StaticDigital *static_digital_array_ptr, int64_t count, int64_t type);

#ifdef __cplusplus
}
#endif

#endif // _WRITE_PLUGIN_H_

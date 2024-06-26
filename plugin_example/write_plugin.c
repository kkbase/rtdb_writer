#include <stdio.h>
#include "write_plugin.h"

// 登陆数据库
void login() {
    printf("rtdb login!\n");
}

// 登出数据库
void logout() {
    printf("rtdb logout!\n");
}

// 写实时模拟量
void write_rt_analog(int64_t crew_id, int64_t time, Analog *analog_array_ptr, int64_t count) {
    printf("write rt analog: crew_id: %lld, time: %lld, count: %lld\n", crew_id, time, count);
}

// 写实时数字量
void write_rt_digital(int64_t crew_id, int64_t time, Digital *digital_array_ptr, int64_t count) {
}

// 写历史模拟量
void write_his_analog(int64_t crew_id, int64_t time, Analog *analog_array_ptr, int64_t count) {
}

// 写历史数字量
void write_his_digital(int64_t crew_id, int64_t time, Digital *digital_array_ptr, int64_t count) {
}

// 写静态模拟量
void write_static_analog(int64_t crew_id, StaticAnalog *static_analog_array_ptr, int64_t count) {
}

// 写静态数字量
void write_static_digital(int64_t crew_id, StaticDigital *static_digital_array_ptr, int64_t count) {
}

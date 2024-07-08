#include <stdio.h>
#include "write_plugin.h"

// 登陆数据库
void login(char *param) {
    if (param != NULL) {
        printf("rtdb login: param: %s\n", param);
    } else {
        printf("rtdb login: param: NULL\n");
    }
}

// 登出数据库
void logout() {
    printf("rtdb logout!\n");
}

// 写实时模拟量
void write_rt_analog(int64_t unit_id, int64_t time, Analog *analog_array_ptr, int64_t count, bool is_fast) {
    if (is_fast) {
        printf("write rt analog(fast): unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
    } else {
        printf("write rt analog(normal): unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
    }
    int sum = 0;
    for (int i=0; i<10000000; i++) {
        sum++;
    }
}

// 写实时数字量
void write_rt_digital(int64_t unit_id, int64_t time, Digital *digital_array_ptr, int64_t count, bool is_fast) {
    if (is_fast) {
        printf("write rt digital(fast): unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
    } else {
        printf("write rt digital(normal): unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
    }
}

// 写实时模拟量
void write_rt_analog_list(int64_t unit_id, int64_t *time, Analog **analog_array_array_ptr, int64_t *array_count, int64_t count) {
    printf("write rt analog: unit_id: %lld, section count: %lld\n", unit_id, count);
}

// 写实时数字量
void write_rt_digital_list(int64_t unit_id, int64_t *time, Digital **digital_array_array_ptr, int64_t *array_count, int64_t count) {
    printf("write rt digital: unit_id: %lld, section count: %lld\n", unit_id, count);
}

// 写历史模拟量
void write_his_analog(int64_t unit_id, int64_t time, Analog *analog_array_ptr, int64_t count) {
    printf("write his analog: unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
    int sum = 0;
    for (int i=0; i<10000000; i++) {
        sum++;
    }
}

// 写历史数字量
void write_his_digital(int64_t unit_id, int64_t time, Digital *digital_array_ptr, int64_t count) {
    printf("write his digital: unit_id: %lld, time: %lld, count: %lld\n", unit_id, time, count);
}

// 写静态模拟量
void write_static_analog(int64_t unit_id, StaticAnalog *static_analog_array_ptr, int64_t count, bool is_fast) {
    if (is_fast) {
        printf("write static analog(fast): unit_id: %lld, count: %lld\n", unit_id, count);
    } else {
        printf("write static analog(normal): unit_id: %lld, count: %lld\n", unit_id, count);
    }
}

// 写静态数字量
void write_static_digital(int64_t unit_id, StaticDigital *static_digital_array_ptr, int64_t count, bool is_fast) {
    if (is_fast) {
        printf("write static digital(fast): unit_id: %lld, count: %lld\n", unit_id, count);
    } else {
        printf("write static digital(normal): unit_id: %lld, count: %lld\n", unit_id, count);
    }
}

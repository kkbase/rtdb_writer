#include <stdio.h>
#include "write_plugin.h"

// 登陆数据库
void login() {
    printf("login db!");
}

// 登出数据库
void logout() {
    printf("logout db!");
}

// 写模拟量
void write_analog(int64_t time, Analog *analog_array_ptr, int64_t count) {
    printf("write analog section: %lld!", time);
}

// 写数字量
void write_digital(int64_t time, Digital *digital_array_ptr, int64_t count) {
    printf("write digital section: %lld!", time);
}

// 写静态模拟量
void write_static_analog(StaticAnalog *static_analog_array_ptr, int64_t count) {
    printf("write static analog");
}

// 写静态数字量
void write_static_digital(StaticDigital *static_digital_array_ptr, int64_t count) {
    printf("write static digital");
}

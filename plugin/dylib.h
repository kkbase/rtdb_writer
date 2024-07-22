#ifndef _C_PLUGIN_H_
#define _C_PLUGIN_H_

#include "write_plugin.h"
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

#ifdef _WIN32
#include <windows.h>
#define LIBRARY_HANDLE HMODULE
#define LOAD_LIBRARY(name) LoadLibrary(name)
#define GET_FUNCTION GetProcAddress
#define CLOSE_LIBRARY FreeLibrary
#else
#include <dlfcn.h>
#define LIBRARY_HANDLE void*
#define LOAD_LIBRARY(name) dlopen(name, RTLD_LAZY)
#define GET_FUNCTION dlsym
#define CLOSE_LIBRARY dlclose
#endif

typedef struct _DYLIB_HANDLE_ {
    LIBRARY_HANDLE handle;
} DYLIB_HANDLE;

DYLIB_HANDLE load_library(char *name) {
    DYLIB_HANDLE handle = {LOAD_LIBRARY(name)};
    return handle;
}

int close_library(DYLIB_HANDLE  handle) {
    return CLOSE_LIBRARY(handle.handle);
}

int dy_login(DYLIB_HANDLE handle, char* param) {
    int (*login)(char*) = (int (*)(char*)) GET_FUNCTION(handle.handle, "login");
    int rtn = login(param);
    printf("debug: c Call Plugin rtn: %d\n", rtn);
    return rtn;
}

void dy_logout(DYLIB_HANDLE handle) {
    void (*logout)() = (void (*)()) GET_FUNCTION(handle.handle, "logout");
    logout();
}

void dy_write_rt_analog(DYLIB_HANDLE handle, int32_t magic, int64_t unit_id, int64_t time, Analog *analog, int64_t count, bool is_fast) {
    void (*write_rt_analog)(int32_t, int64_t, int64_t, Analog*, int64_t, bool) = (void (*)(int32_t, int64_t, int64_t, Analog*, int64_t, bool)) GET_FUNCTION(handle.handle, "write_rt_analog");
    write_rt_analog(magic, unit_id, time, analog, count, is_fast);
}

void dy_write_rt_digital(DYLIB_HANDLE handle, int32_t magic, int64_t unit_id, int64_t time, Digital *digital, int64_t count, bool is_fast) {
    void (*write_rt_digital)(int64_t, int64_t, int64_t, Digital*, int64_t, bool) = (void (*)(int64_t, int64_t, int64_t, Digital*, int64_t, bool)) GET_FUNCTION(handle.handle, "write_rt_digital");
    write_rt_digital(magic, unit_id, time, digital, count, is_fast);
}

void dy_write_rt_analog_list(DYLIB_HANDLE handle, int32_t magic, int64_t unit_id, int64_t *time, Analog **analog_array_array_ptr, int64_t *array_count, int64_t count) {
    void (*write_rt_analog_list)(int32_t, int64_t, int64_t*, Analog**, int64_t*, int64_t) = (void (*)(int32_t, int64_t, int64_t*, Analog**, int64_t*, int64_t)) GET_FUNCTION(handle.handle, "write_rt_analog_list");
    write_rt_analog_list(magic, unit_id, time, analog_array_array_ptr, array_count, count);
}

void dy_write_rt_digital_list(DYLIB_HANDLE handle, int32_t magic, int64_t unit_id, int64_t *time, Digital **digital_array_array_ptr, int64_t *array_count, int64_t count) {
    void (*write_rt_digital_list)(int32_t, int64_t, int64_t*, Digital**, int64_t*, int64_t) = (void (*)(int32_t, int64_t, int64_t*, Digital**, int64_t*, int64_t)) GET_FUNCTION(handle.handle, "write_rt_digital_list");
    write_rt_digital_list(magic, unit_id, time, digital_array_array_ptr, array_count, count);
}


void dy_write_his_analog(DYLIB_HANDLE handle, int32_t magic, int64_t unit_id, int64_t time, Analog *analog, int64_t count) {
    void (*write_his_analog)(int32_t, int64_t, int64_t, Analog*, int64_t) = (void (*)(int32_t, int64_t, int64_t, Analog*, int64_t)) GET_FUNCTION(handle.handle, "write_his_analog");
    write_his_analog(magic, unit_id, time, analog, count);
}

void dy_write_his_digital(DYLIB_HANDLE handle, int32_t magic, int64_t unit_id, int64_t time, Digital *digital, int64_t count) {
    void (*write_his_digital)(int32_t, int64_t, int64_t, Digital*, int64_t) = (void (*)(int32_t, int64_t, int64_t, Digital*, int64_t)) GET_FUNCTION(handle.handle, "write_his_digital");
    write_his_digital(magic, unit_id, time, digital, count);
}

void dy_write_static_analog(DYLIB_HANDLE handle, int32_t magic, int64_t unit_id, StaticAnalog *static_analog, int64_t count, int64_t type) {
    void (*write_static_analog)(int32_t, int64_t, StaticAnalog*, int64_t, int64_t) = (void (*)(int32_t, int64_t, StaticAnalog*, int64_t, int64_t)) GET_FUNCTION(handle.handle, "write_static_analog");
    write_static_analog(magic, unit_id, static_analog, count, type);
}

void dy_write_static_digital(DYLIB_HANDLE handle, int32_t magic, int64_t unit_id, StaticDigital *static_digital, int64_t count, int64_t type) {
    void (*write_static_digital)(int32_t, int64_t, StaticDigital*, int64_t, int64_t) = (void (*)(int32_t, int64_t, StaticDigital*, int64_t, int64_t)) GET_FUNCTION(handle.handle, "write_static_digital");
    write_static_digital(magic, unit_id, static_digital, count, type);
}


#ifdef __cplusplus
}
#endif

#endif // _C_PLUGIN_H_
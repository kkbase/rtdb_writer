#ifndef _C_PLUGIN_H_
#define _C_PLUGIN_H_

#include "write_plugin.h"

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

LIBRARY_HANDLE load_library(char *name) {
    return LOAD_LIBRARY(name);
}

int close_library(LIBRARY_HANDLE handle) {
    return CLOSE_LIBRARY(handle);
}

void dy_login(LIBRARY_HANDLE handle) {
    void (*login)() = (void (*)()) GET_FUNCTION(handle, "login");
    login();
}

void dy_logout(LIBRARY_HANDLE handle) {
    void (*logout)() = (void (*)()) GET_FUNCTION(handle, "logout");
    logout();
}

void dy_write_analog(LIBRARY_HANDLE handle, int64_t time, Analog *analog, int64_t count) {
    void (*write_analog)(int64_t, Analog*, int64_t) = (void (*)(int64_t, Analog*, int64_t)) GET_FUNCTION(handle, "write_analog");
    write_analog(time, analog, count);
}

void dy_write_static_analog(LIBRARY_HANDLE handle, StaticAnalog *static_analog, int64_t count) {
    void (*write_static_analog)(StaticAnalog*, int64_t) = (void (*)(StaticAnalog*, int64_t)) GET_FUNCTION(handle, "write_static_analog");
    write_static_analog(static_analog, count);
}

void dy_write_digital(LIBRARY_HANDLE handle, int64_t time, Digital *digital, int64_t count) {
    void (*write_digital)(int64_t, Digital*, int64_t) = (void (*)(int64_t, Digital*, int64_t)) GET_FUNCTION(handle, "write_digital");
    write_digital(time, digital, count);
}


void dy_write_static_digital(LIBRARY_HANDLE handle, StaticDigital *static_digital, int64_t count) {
    void (*write_static_digital)(StaticDigital*, int64_t) = (void (*)(StaticDigital*, int64_t)) GET_FUNCTION(handle, "write_static_digital");
    write_static_digital(static_digital, count);
}


#ifdef __cplusplus
}
#endif

#endif // _C_PLUGIN_H_
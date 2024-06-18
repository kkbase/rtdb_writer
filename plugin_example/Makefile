CC := gcc
CFLAGS := -fPIC
LDFLAGS := -shared
TARGET := libplugin
GO := go

# Detect the operating system
ifeq ($(OS),Windows_NT)
    C_TARGET := cwrite_plugin.dll
    GO_TARGET := gowrite_plugin.dll
else
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Linux)
        C_TARGET := libcwrite_plugin.so
        GO_TARGET := libgowrite_plugin.so
    endif
    ifeq ($(UNAME_S),Darwin)
        TARGET := libcwrite_plugin.dylib
        GO_TARGET := libgowrite_plugin.dylib
    endif
endif

# Source files
SRC := write_plugin.c

# Default target
all: $(GO_TARGET)

$(GO_TARGET): $(C_TARGET)
	$(GO) build -buildmode=plugin -o $(GO_TARGET) write_plugin.go

# Build target for shared library
$(C_TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# Clean target
clean:
	rm -f *.so *.dll *.dylib

.PHONY: all clean
CC := gcc
CFLAGS := -fPIC
LDFLAGS := -shared
TARGET := libplugin

# Detect the operating system
ifeq ($(OS),Windows_NT)
    TARGET := plugin.dll
else
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Linux)
        TARGET := libplugin.so
    endif
    ifeq ($(UNAME_S),Darwin)
        TARGET := libplugin.dylib
    endif
endif

# Source files
SRC := write_plugin.c

# Default target
all: $(TARGET)

# Build target for shared library
$(TARGET): $(SRC)
	$(CC) $(CFLAGS) $(LDFLAGS) -o $@ $^

# Clean target
clean:
	rm -f *.so *.dll *.dylib

.PHONY: all clean
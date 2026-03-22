# TurboGraphDB Makefile
# High-Performance Graph Database Engine

CC = gcc
CFLAGS = -O3 -Wall -Wextra -fPIC -std=c11 -DNDEBUG
LDFLAGS = -shared
DEBUG_FLAGS = -g -O0 -DDEBUG

# Directories
SRC_DIR = src
INCLUDE_DIR = include
BUILD_DIR = build
LIB_DIR = lib

# Source files
SOURCES = $(wildcard $(SRC_DIR)/*.c)
OBJECTS = $(SOURCES:$(SRC_DIR)/%.c=$(BUILD_DIR)/%.o)

# Output library
LIBRARY = $(LIB_DIR)/libgraphdb.so

# Default target
all: directories $(LIBRARY)

# Create directories
directories:
	@mkdir -p $(BUILD_DIR) $(LIB_DIR)

# Compile object files
$(BUILD_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -I$(INCLUDE_DIR) -c $< -o $@

# Build shared library
$(LIBRARY): $(OBJECTS)
	$(CC) $(LDFLAGS) -o $@ $^

# Debug build
debug: CFLAGS = $(DEBUG_FLAGS) -fPIC -std=c11
debug: clean all

# Clean
clean:
	rm -rf $(BUILD_DIR) $(LIB_DIR)

# Install (copy to system)
install: $(LIBRARY)
	cp $(LIBRARY) /usr/local/lib/
	cp $(INCLUDE_DIR)/graph.h /usr/local/include/
	ldconfig

# Uninstall
uninstall:
	rm -f /usr/local/lib/libgraphdb.so
	rm -f /usr/local/include/graph.h
	ldconfig

# Test
test: $(LIBRARY)
	@echo "Building tests..."
	$(CC) $(CFLAGS) -I$(INCLUDE_DIR) tests/test_graph.c -L$(LIB_DIR) -lgraphdb -o $(BUILD_DIR)/test_graph
	@echo "Running tests..."
	LD_LIBRARY_PATH=$(LIB_DIR) $(BUILD_DIR)/test_graph

# Benchmark
benchmark: $(LIBRARY)
	@echo "Building benchmark..."
	$(CC) $(CFLAGS) -I$(INCLUDE_DIR) tests/benchmark.c -L$(LIB_DIR) -lgraphdb -o $(BUILD_DIR)/benchmark
	@echo "Running benchmark..."
	LD_LIBRARY_PATH=$(LIB_DIR) $(BUILD_DIR)/benchmark

.PHONY: all directories clean install uninstall test benchmark debug

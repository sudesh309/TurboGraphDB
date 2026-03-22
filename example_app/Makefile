CC      = gcc
CFLAGS  = -O2 -Wall -std=c11
INC     = -I../graph-engine/include
LIB_DIR = ../graph-engine/lib
LDFLAGS = -L$(LIB_DIR) -lgraphdb -lm

all: social_network

social_network: social_network.c
	$(CC) $(CFLAGS) $(INC) $< $(LDFLAGS) -o $@

run: social_network
	LD_LIBRARY_PATH=$(LIB_DIR) ./social_network

clean:
	rm -f social_network

.PHONY: all run clean

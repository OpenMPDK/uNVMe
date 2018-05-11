ifeq ($(KV_DIR),)
$(error KV_DIR is not defined)
endif

ifeq ($(wildcard $(KV_DIR)),)
$(error KV_DIR points to an invalid location)
endif

include $(KV_DIR)/config_kv

KV_DD_LIB = $(KV_DIR)/core/libkvnvmedd.a

GLOBAL_INCLUDE = $(KV_DIR)/../include
COMMON_PATH = ../../external/common
KV_LIBS = -lm -pthread -lrt -ldl

OBJS = $(C_SRCS:.c=.o)
LINK_C = $(CC) -o $@ $(LDFLAGS) $(OBJS) $(LIBS)
LIB_C = ar crDs $@ $(OBJS)

#KV_LIBS = -lm -pthread -lrt -ldl -lnuma  //for dkdp17.11

CFLAGS += -Wall -Wextra -Wno-unused-parameter -march=native -m64 -I$(KV_DIR)/include -D_GNU_SOURCE -fPIC -std=gnu99 -I$(GLOBAL_INCLUDE) -I$(KV_DIR)/external/spdk/include -I$(KV_DIR)/external/common

ifeq ($(CONFIG_DEBUG),y)
CFLAGS += -g -O0
LDFLAGS += -O0
DEBUG=y
else
CFLAGS += -O2
LDFLAGS += -O2
DEBUG=n
endif

ifndef ($(CONFIG_KV_LOG_LEVEL))
CFLAGS += -DKVNVME_LOG_LEVEL=$(CONFIG_KVNVME_LOG_LEVEL)
endif

ifeq ($(CONFIG_LBA_SSD), y)
CFLAGS += -DLBA_SSD
endif

LDFLAGS+= -Wl,-z,relro,-z,now -Wl,-z,noexecstack

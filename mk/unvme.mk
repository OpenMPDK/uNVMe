ifeq ($(KV_DIR),)
$(error KV_DIR is not defined)
endif

ifeq ($(wildcard $(KV_DIR)),)
$(error KV_DIR points to an invalid location)
endif

#include $(KV_DIR)/config_kv

# ~MPDK v18.07
#SPDK_PATH = spdk
#DPDK_PATH = dpdk
#MAKEFILE = Makefile.mpdk_18.07

# MPDK v18.07~
SPDK_PATH = spdk-18.04.1
DPDK_PATH = dpdk-18.05
MAKEFILE = Makefile

CONFIG_DEBUG=n
# KV NVMe Driver Log Level
# 1. Error, 2. Warning, 3. Info, 4. Debug
CONFIG_KVNVME_LOG_LEVEL=3


KV_DD_LIB = $(KV_DIR)/driver/core/libkvnvmedd.a
GLOBAL_INCLUDE = $(KV_DIR)/include
COMMON_INCLUDE = $(KV_DIR)/driver/external/common
KV_LIBS = -lm -pthread -lrt -ldl -lnuma -luuid
OBJS = $(C_SRCS:.c=.o)
LINK_C = $(CC) -o $@ $(LDFLAGS) $(OBJS) $(LIBS)
LIB_C = ar crDs $@ $(OBJS)
CFLAGS += -Wall -Wextra -Wno-unused-parameter -march=native -m64 -I$(KV_DIR)/driver/include -D_GNU_SOURCE -fPIC -std=gnu99 -I$(GLOBAL_INCLUDE) -I$(KV_DIR)/driver/external/$(SPDK_PATH)/include -I$(COMMON_INCLUDE)



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

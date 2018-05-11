ifeq ($(KV_DIR),)
export KV_DIR = $(shell pwd)/../../
endif

include $(KV_DIR)/mk/kv.dd.mk

C_SRCS := udd_perf_async.c $(COMMON_PATH)/EagleHashIP.c $(COMMON_PATH)/latency_stat.c $(COMMON_PATH)/kvutil.c

LIBS += $(KV_DD_LIB) $(KV_LIBS)

CFLAGS += -O0 -I/home/test/jhim/KV_DD/user/spdk/include -march=native -m64 -D_GNU_SOURCE -I../../include -I$(COMMON_PATH) -std=gnu99

APP = udd_perf_async

$(APP): $(OBJS) $(KV_DD_LIB)
	$(LINK_C)

all: $(APP)

clean:
	rm -f $(OBJS) $(APP)
	rm -f $(KV_DIR)/build/debug/bin/$(APP)
	rm -f $(KV_DIR)/build/release/bin/$(APP)

install:
ifeq ($(DEBUG),y)
	cp $(APP) $(KV_DIR)/build/debug/bin/
else
	cp $(APP) $(KV_DIR)/build/release/bin/
endif

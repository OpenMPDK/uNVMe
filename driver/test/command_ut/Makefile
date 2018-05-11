ifeq ($(KV_DIR),)
export KV_DIR = $(shell pwd)/../../
endif

include $(KV_DIR)/mk/kv.dd.mk
KV_DD_LIB += $(KV_DIR)/core/libkvnvmedd.a

C_SRCS := exist_append_iterate_getlog.c  $(COMMON_PATH)/kvutil.c $(COMMON_PATH)/EagleHashIP.c $(COMMON_PATH)/latency_stat.c

LIBS += $(KV_DD_LIB) $(KV_LIBS)

CFLAGS += -I$(HASH_PATH) -Wno-missing-braces -Wno-sign-compare

APP = exist_append_iterate_getlog

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

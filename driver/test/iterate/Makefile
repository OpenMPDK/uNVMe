ifeq ($(KV_DIR),)
export KV_DIR = $(shell pwd)/../../
endif

include $(KV_DIR)/mk/kv.dd.mk

C_SRCS := iterate.c  $(COMMON_PATH)/kvutil.c $(COMMON_PATH)/EagleHashIP.c $(COMMON_PATH)/latency_stat.c

LIBS += $(KV_DD_LIB) $(KV_LIBS)

CFLAGS += -I$(HASH_PATH) -Wno-missing-braces -Wno-sign-compare

APP = iterate

$(APP): $(OBJS) $(KV_DD_LIB)
	$(LINK_C)

all: $(APP)

clean:
	rm -f *.o $(APP)
	rm -f $(KV_DIR)/build/debug/bin/$(APP)
	rm -f $(KV_DIR)/build/release/bin/$(APP)

install:
ifeq ($(DEBUG),y)
	cp $(APP) $(KV_DIR)/build/debug/bin/
else
	cp $(APP) $(KV_DIR)/build/release/bin/
endif

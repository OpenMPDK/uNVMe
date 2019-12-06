/**
 *   BSD LICENSE
 *
 *   Copyright (c) 2017 Samsung Electronics Co., Ltd.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Samsung Electronics Co., Ltd. nor the names of
 *       its contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <stdio.h>

#include "kvlog.h"
#include "kvutil.h"

struct logger {
	char *name;  /* log file name */
	int  level;  /* log level */
	int  fd;     /* log file descriptor */
	int  nerror; /* # log error */
};
static struct logger logger;

static int _vscnprintf(char *buf, size_t size, const char *fmt, va_list args){
	int n;

	n = vsnprintf(buf, size, fmt, args);

	/*
	 * The return value is the number of characters which would be written
	 * into buf not including the trailing '\0'. If size is == 0 the
	 * function returns 0.
	 *
	 * On error, the function also returns 0. This is to allow idiom such
	 * as len += _vscnprintf(...)
	 *
	 * See: http://lwn.net/Articles/69419/
	 */
	if (n <= 0) {
		return 0;
	}

	if (n < (int) size) {
		return n;
	}

	return (int)(size - 1);
}

static int _scnprintf(char *buf, size_t size, const char *fmt, ...){
	va_list args;
	int n;

	va_start(args, fmt);
	n = _vscnprintf(buf, size, fmt, args);
	va_end(args);

	return n;
}

int log_init(int level, char *name){
	struct logger *l = &logger;

	l->level = MAX(KV_LOG_EMERG, MIN(level, KV_LOG_DEBUG));
	l->name = name;
	if (name == NULL || !strlen(name)) {
		l->fd = STDERR_FILENO;
	} else {
		l->fd = open(name, O_WRONLY | O_APPEND | O_CREAT, 0644);
		if (l->fd < 0) {
			log_stderr("opening log file '%s' failed: %s", name,
					strerror(errno));
			return -1;
		}
	}

	return 0;
}

void log_deinit(void){
	struct logger *l = &logger;

	if (l->fd != STDERR_FILENO) {
		close(l->fd);
	}
}

void log_reopen(void){
	struct logger *l = &logger;

	if (l->fd != STDERR_FILENO) {
		close(l->fd);
		l->fd = open(l->name, O_WRONLY | O_APPEND | O_CREAT, 0644);
		if (l->fd < 0) {
			log_stderr("reopening log file '%s'failed, ignored: %s", l->name,
					strerror(errno));
		}
	}
}

void log_level_up(void){
	struct logger *l = &logger;

	if (l->level < KV_LOG_DEBUG) {
		l->level++;
		loga("up log level to %d", l->level);
	}
}

void log_level_down(void){
	struct logger *l = &logger;

	if (l->level > KV_LOG_EMERG) {
		l->level--;
		loga("down log level to %d", l->level);
	}
}

void log_level_set(int level){
	struct logger *l = &logger;

	l->level = MAX(KV_LOG_EMERG, MIN(level, KV_LOG_DEBUG));
	loga("set log level to %d", l->level);
}

int log_loggable(int level){
	struct logger *l = &logger;

	if (level > l->level) {
		return 0;
	}

	return 1;
}

void _log_internal(const char *file, int line, int panic, const char *fmt, ...){
	struct logger *l = &logger;
	int len, size, errno_save;
	char buf[KV_LOG_MAX_LEN], *timestr;
	va_list args;
	struct tm *local;
	time_t t;
	ssize_t n;

	if (l->fd < 0) {
		return;
	}

	errno_save = errno;
	len = 0;            /* length of output buffer */
	size = KV_LOG_MAX_LEN; /* size of output buffer */

	t = time(NULL);
	local = localtime(&t);
	timestr = asctime(local);

	len += _scnprintf(buf + len, size - len, "[%.*s] %s:%d ",
			strlen(timestr) - 1, timestr, file, line);

	va_start(args, fmt);
	len += _vscnprintf(buf + len, size - len, fmt, args);
	va_end(args);

	buf[len++] = '\n';

	n = write(l->fd, buf, len);
	if (n < 0) {
		l->nerror++;
	}

	errno = errno_save;

	if (panic) {
		abort();
	}
}

void _log_stderr_internal(const char *fmt, ...){
	struct logger *l = &logger;
	int len, size, errno_save;
	char buf[4 * KV_LOG_MAX_LEN];
	va_list args;
	ssize_t n;

	errno_save = errno;
	len = 0;                /* length of output buffer */
	size = 4 * KV_LOG_MAX_LEN; /* size of output buffer */

	va_start(args, fmt);
	len += _vscnprintf(buf, size, fmt, args);
	va_end(args);

	buf[len++] = '\n';

	n = write(STDERR_FILENO, buf, len);
	if (n < 0) {
		l->nerror++;
	}

	errno = errno_save;
}

/*
 * Hexadecimal dump in the canonical hex + ascii display
 * See -C option in man hexdump
 */
void _log_hexdump_internal(const char *file, int line, char *data, int datalen, const char *fmt, ...){
	struct logger *l = &logger;
	char buf[8 * KV_LOG_MAX_LEN];
	int i, off, len, size, errno_save;
	va_list args;
	ssize_t n;

	if (l->fd < 0) {
		return;
	}

	/* log format */
	va_start(args, fmt);
	_log_internal(file, line, 0, fmt);
	va_end(args);

	/* log hexdump */
	errno_save = errno;
	off = 0;                  /* data offset */
	len = 0;                  /* length of output buffer */
	size = 8 * KV_LOG_MAX_LEN;   /* size of output buffer */

	while (datalen != 0 && (len < size - 1)) {
		char *save, *str;
		unsigned char c;
		int savelen;

		len += _scnprintf(buf + len, size - len, "%08x  ", off);

		save = data;
		savelen = datalen;

		for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {
			c = (unsigned char)(*data);
			str = (i == 7) ? "  " : " ";
			len += _scnprintf(buf + len, size - len, "%02x%s", c, str);
		}
		for ( ; i < 16; i++) {
			str = (i == 7) ? "  " : " ";
			len += _scnprintf(buf + len, size - len, "  %s", str);
		}

		data = save;
		datalen = savelen;

		len += _scnprintf(buf + len, size - len, "  |");

		for (i = 0; datalen != 0 && i < 16; data++, datalen--, i++) {
			c = (unsigned char)(isprint(*data) ? *data : '.');
			len += _scnprintf(buf + len, size - len, "%c", c);
		}
		len += _scnprintf(buf + len, size - len, "|\n");

		off += 16;
	}

	n = write(l->fd, buf, len);
	if (n < 0) {
		l->nerror++;
	}

	errno = errno_save;
}

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
#ifndef _KV_LOG_H_
#define _KV_LOG_H_


#define KV_LOG_MAX_LEN 256 /* max length of log message */
#define KV_LOG_EMERG   0   /* critical conditions */
#define KV_LOG_ERR     1   /* error conditions */
#define KV_LOG_INFO    2   /* informational */
#define KV_LOG_DEBUG   3   /* debug messages */

int log_init(int level, char *filename);
void log_deinit(void);
void log_level_up(void);
void log_level_down(void);
void log_level_set(int level);
void log_reopen(void);
int log_loggable(int level);
void _log_internal(const char *file, int line, int panic, const char *fmt, ...);
void _log_stderr_internal(const char *fmt, ...);
void _log_hexdump_internal(const char *file, int line, char *data, int data_len, const char *fmt, ...);

/*
 * log_debug         - debug log messages based on a log level
 * log_hexdump    - hexadump -C of a log buffer
 * log_stderr         - log to stderr
 * loga                  - log always
 * loga_hexdump  - log hexdump always
 * log_error          - error log messages
 * log_panic          - log messages followed by a panic
 * ...
 */
#define log_debug(_level, ...) do {                                      \
  if (log_loggable(_level) != 0) {                                       \
    _log_internal(__FILE__, __LINE__, 0, __VA_ARGS__);                   \
  }                                                                      \
} while (0);

#define log_hexdump(_level, _data, _data_len, ...) do {                  \
   if (log_loggable(_level) != 0) {                                      \
    _log_hexdump_internal(__FILE__, __LINE__, (char *)(_data), (int)(_data_len), __VA_ARGS__);           \
  }                                                                      \
} while (0);

#define log_stderr(...) do {                                             \
  _log_stderr_internal(__VA_ARGS__);                                     \
} while (0);

#define loga(...) do {                                                   \
  _log_internal(__FILE__, __LINE__, 0, __VA_ARGS__);                     \
} while (0);

#define loga_hexdump(_data, _data_len, ...) do {                         \
  _log_hexdump_internal(__FILE__, __LINE__, (char *)(_data),             \
    (int)(_data_len), __VA_ARGS__);                                       \
} while (0);                                                             \

#define log_error(...) do {                                              \
  if (log_loggable(KV_LOG_ERR) != 0) {                                   \
    _log_internal(__FILE__, __LINE__, 0, __VA_ARGS__);                   \
  }                                                                      \
} while (0);

//#define log_warn(...) do {                                                                                         \
//    if (log_loggable(LOG_WARN) != 0) {                                                                      \
//        _log(__FILE__, __LINE__, 0, __VA_ARGS__);                                                     \
//    }                                                                                                                            \
//} while (0);

#define log_panic(...) do {                                               \
  if (log_loggable(KV_LOG_EMERG) != 0) {                                  \
    _log_internal(__FILE__, __LINE__, 1, __VA_ARGS__);                    \
  }                                                                       \
} while (0);

#endif

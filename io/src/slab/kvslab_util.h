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

/*
 * kvslab_util.h
 */

#ifndef KVSLAB_UTIL_H_
#define KVSLAB_UTIL_H_

/*
 * Make data 'd' or pointer 'p', n-byte aligned, where n is a power of 2
 */
#define KVSLAB_ALIGNMENT        sizeof(unsigned long) /* platform word */
#define KVSLAB_ASSERT(_x) do {	\
	if (!(_x)) {		\
		abort();	\
	}			\
} while (0)


/*
 * Singly-linked Tail queue functions.
 */
#define KVSLAB_STAILQ_FIRST(head)    ((head)->stqh_first)

#define KVSLAB_STAILQ_EMPTY(head)    ((head)->stqh_first == NULL)

#define KVSLAB_STAILQ_NEXT(elm, field)    ((elm)->field.stqe_next)

#define KVSLAB_STAILQ_HEAD(name, type)                                  \
struct name {                                                           \
    struct type *stqh_first; /* first element */                        \
    struct type **stqh_last; /* addr of last next element */            \
}

#define KVSLAB_STAILQ_ENTRY(type)                                       \
struct {                                                                \
    struct type *stqe_next;    /* next element */                       \
}

#define KVSLAB_STAILQ_INIT(head) do {                                   \
    KVSLAB_STAILQ_FIRST((head)) = NULL;                                 \
    (head)->stqh_last = &KVSLAB_STAILQ_FIRST((head));                   \
} while (0)

#define KVSLAB_STAILQ_INSERT_HEAD(head, elm, field) do {				\
    if ((KVSLAB_STAILQ_NEXT((elm), field) = KVSLAB_STAILQ_FIRST((head))) == NULL)	\
        (head)->stqh_last = &KVSLAB_STAILQ_NEXT((elm), field);				\
    KVSLAB_STAILQ_FIRST((head)) = (elm);						\
} while (0)

#define KVSLAB_STAILQ_INSERT_TAIL(head, elm, field) do {		\
    KVSLAB_STAILQ_NEXT((elm), field) = NULL;                            \
    *(head)->stqh_last = (elm);						\
    (head)->stqh_last = &KVSLAB_STAILQ_NEXT((elm), field);              \
} while (0)

#define KVSLAB_STAILQ_REMOVE(head, field) do {                            	\
    if ((KVSLAB_STAILQ_FIRST((head)) =                                          \
         KVSLAB_STAILQ_NEXT(KVSLAB_STAILQ_FIRST((head)), field)) == NULL) {     \
        (head)->stqh_last = &KVSLAB_STAILQ_FIRST((head));                      	\
    }										\
} while (0)


/*
 * Tail queue declarations.
 */
#define KVSLAB_TAILQ_HEAD(name, type)                           \
struct name {                                                   \
    struct type *tqh_first; /* first element */                 \
    struct type **tqh_last; /* addr of last next element */     \
}

#define KVSLAB_TAILQ_FIRST(head)    ((head)->tqh_first)

#define KVSLAB_TAILQ_INIT(head) do {                            \
    KVSLAB_TAILQ_FIRST((head)) = NULL;                          \
    (head)->tqh_last = &KVSLAB_TAILQ_FIRST((head));             \
} while (0)

#define KVSLAB_TAILQ_ENTRY(type)                                        \
struct {                                                                \
    struct type *tqe_next;  /* next element */                          \
    struct type **tqe_prev; /* address of previous next element */      \
}

#define KVSLAB_TAILQ_NEXT(elm, field) ((elm)->field.tqe_next)

#define KVSLAB_TAILQ_INSERT_TAIL(head, elm, field) do {                 \
    KVSLAB_TAILQ_NEXT((elm), field) = NULL;                             \
    (elm)->field.tqe_prev = (head)->tqh_last;                           \
    *(head)->tqh_last = (elm);                                          \
    (head)->tqh_last = &KVSLAB_TAILQ_NEXT((elm), field);                \
} while (0)

#define KVSLAB_TAILQ_INSERT_HEAD(head, elm, field) do {				\
    if ((KVSLAB_TAILQ_NEXT((elm), field) = KVSLAB_TAILQ_FIRST((head))) != NULL)	\
        KVSLAB_TAILQ_FIRST((head))->field.tqe_prev =				\
            &KVSLAB_TAILQ_NEXT((elm), field);					\
    else									\
        (head)->tqh_last = &KVSLAB_TAILQ_NEXT((elm), field);			\
    KVSLAB_TAILQ_FIRST((head)) = (elm);						\
    (elm)->field.tqe_prev = &KVSLAB_TAILQ_FIRST((head));			\
} while (0)

#define KVSLAB_TAILQ_REMOVE(head, elm, field) do {				\
    if ((KVSLAB_TAILQ_NEXT((elm), field)) != NULL) {				\
        KVSLAB_TAILQ_NEXT((elm), field)->field.tqe_prev =			\
            (elm)->field.tqe_prev;						\
    } else {									\
        (head)->tqh_last = (elm)->field.tqe_prev;				\
    }										\
    *(elm)->field.tqe_prev = KVSLAB_TAILQ_NEXT((elm), field);			\
} while (0)

#define KVSLAB_TAILQ_EMPTY(head)    ((head)->tqh_first == NULL)

#define KVSLAB_TAILQ_FOREACH(var, head, field)                          \
    for ((var) = KVSLAB_TAILQ_FIRST((head));                            \
        (var);                                                          \
        (var) = KVSLAB_TAILQ_NEXT((var), field))

#endif /* KVSLAB_UTIL_H_ */

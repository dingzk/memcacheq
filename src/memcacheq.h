/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  MemcacheQ - Simple Queue Service over Memcache
 *
 *      http://memcacheq.googlecode.com
 *
 *  The source code of MemcacheQ is most based on MemcachDB:
 *
 *      http://memcachedb.googlecode.com
 *
 *  Copyright 2008 Steve Chu.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      Steve Chu <stvchu@gmail.com>
 *
 */

#include "config.h"

#include "hash.h"
#include "hashtable.h"
#include "pq_manager.h"
#include "pqueue.h"
#include "logger.h"

#include <event.h>
#include <jemalloc/jemalloc.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>

#define DATA_BUFFER_SIZE 2048
#define UDP_READ_BUFFER_SIZE 65536
#define UDP_MAX_PAYLOAD_SIZE 1400
#define UDP_HEADER_SIZE 8
#define MAX_SENDBUF_SIZE (256 * 1024 * 1024)
/* I'm told the max legnth of a 64-bit num converted to string is 20 bytes.
 * Plus a few for spaces, \r\n, \0 */
#define SUFFIX_SIZE 24

/** Initial size of list of items being returned by "get". */
#define ITEM_LIST_INITIAL 200

/** Initial size of the sendmsg() scatter/gather array. */
#define IOV_LIST_INITIAL 400

/** Initial number of sendmsg() argument structures to allocate. */
#define MSG_LIST_INITIAL 10

/** High water marks for buffer shrinking */
#define READ_BUFFER_HIGHWAT 8192
#define ITEM_LIST_HIGHWAT 400
#define IOV_LIST_HIGHWAT 600
#define MSG_LIST_HIGHWAT 100

#define STAT_KEY_LEN 128
#define STAT_VAL_LEN 128

/** Append a simple stat with a stat name, value format and value */
#define APPEND_STAT(name, fmt, val) append_stat(name, add_stats, c, fmt, val);

/** Append an indexed stat with a stat name (with format), value format
    and value */
#define APPEND_NUM_FMT_STAT(name_fmt, num, name, fmt, val)                     \
    klen = snprintf(key_str, STAT_KEY_LEN, name_fmt, num, name);               \
    vlen = snprintf(val_str, STAT_VAL_LEN, fmt, val);                          \
    add_stats(key_str, klen, val_str, vlen, c);

/** Common APPEND_NUM_FMT_STAT format. */
#define APPEND_NUM_STAT(num, name, fmt, val)                                   \
    APPEND_NUM_FMT_STAT("%d:%s", num, name, fmt, val)

struct stats {
    unsigned int  curr_conns;
    unsigned int  total_conns;
    unsigned int  conn_structs;
    uint64_t      get_cmds;
    uint64_t      get_hits;
    uint64_t      set_cmds;
    uint64_t      set_hits;
    time_t        started;          /* when the process was started */
    uint64_t      bytes_read;
    uint64_t      bytes_written;
};

#define MAX_VERBOSITY_LEVEL 2

struct settings {
    int maxconns;
    int port;
    int udpport;
    char *inter;
    int verbose;
    char *socketpath;   /* path to unix socket if using local socket */
    int access;  /* access mask (a la chmod) for unix domain socket */
    int num_threads;        /* number of libevent threads to run */
    int idle_timeout;       /* Number of seconds to let connections idle */
};

extern struct stats stats;
extern struct settings settings;

enum conn_states {
    conn_listening,  /** the socket which listens for connections */
    conn_read,       /** reading in a command line */
    conn_write,      /** writing out a simple response */
    conn_nread,      /** reading in a fixed number of bytes */
    conn_swallow,    /** swallowing unnecessary bytes w/o storing */
    conn_closing,    /** closing this connection */
    conn_mwrite,     /** writing out many items sequentially */
    conn_closed,     /** connection is closed */
    conn_watch,      /** held by the logger thread as a watcher */
};

enum close_reasons {
    ERROR_CLOSE,
    NORMAL_CLOSE,
    IDLE_TIMEOUT_CLOSE,
    SHUTDOWN_CLOSE,
};

/*
 * Each libevent instance has a wakeup pipe, which other threads
 * can use to signal that they've put a new connection on its queue.
 */
typedef struct {
    pthread_t thread_id;         /* unique ID of this thread */
    struct event_base *base;     /* libevent handle this thread uses */
    struct event notify_event;   /* listen event for notify pipe */
    int notify_receive_fd;       /* receiving end of notify pipe */
    int notify_send_fd;          /* sending end of notify pipe */
    struct conn_queue *ev_queue; /* queue of new connections to handle */
    logger *l;                   /* logger buffer */
} LIBEVENT_THREAD;

/**
 * Callback for any function producing stats.
 *
 * @param key the stat's key
 * @param klen length of the key
 * @param val the stat's value in an ascii form (e.g. text form of a number)
 * @param vlen length of the value
 * @parm cookie magic callback cookie
 */
typedef void (*ADD_STAT)(const char *key, const uint16_t klen,
                         const char *val, const uint32_t vlen,
                         const void *cookie);

typedef struct conn conn;
struct conn {
    int    sfd;
    int    state;
    rel_time_t last_cmd_time;
    struct event event;
    short  ev_flags;
    short  which;   /** which events were just triggered */

    char   *rbuf;   /** buffer to read commands into */
    char   *rcurr;  /** but if we parsed some already, this is where we stopped */
    int    rsize;   /** total allocated size of rbuf */
    int    rbytes;  /** how much data, starting from rcur, do we have unparsed */

    char   *wbuf;
    char   *wcurr;
    int    wsize;
    int    wbytes;
    int    write_and_go; /** which state to go into after finishing current write */
    void   *write_and_free; /** free this memory after finishing writing */

    char   *rkey;   /** when we read in an command's key, it goes here **/
    char   *ritem;  /** when we read in an item's value, it goes here */
    int    rlbytes;

    /* data for the nread state */

    /**
     * item is used to hold an item structure created after reading the command
     * line of set/add/replace commands, but before we finished reading the actual
     * data. The data is read into ITEM_data(item) to avoid extra copying.
     */

    void   *item;     /* for commands set/add/replace  */
    int    item_comm; /* which one is it: set/add/replace */

    /* data for the swallow state */
    int    sbytes;    /* how many bytes to swallow */

    /* data for the mwrite state */
    struct iovec *iov;
    int    iovsize;   /* number of elements allocated in iov[] */
    int    iovused;   /* number of elements used in iov[] */

    struct msghdr *msglist;
    int    msgsize;   /* number of elements allocated in msglist[] */
    int    msgused;   /* number of elements used in msglist[] */
    int    msgcurr;   /* element in msglist[] being transmitted now */
    int    msgbytes;  /* number of bytes in current msg */

    item   **ilist;   /* list of items to write out */
    int    isize;
    item   **icurr;
    int    ileft;

    enum close_reasons close_reason; /* reason for transition into conn_closing */

    /* current stats command */
    struct {
        char *buffer;
        size_t size;
        size_t offset;
    } stats;

    /* data for UDP clients */
    bool   udp;       /* is this is a UDP "connection" */
    int    request_id; /* Incoming UDP request ID, if this is a UDP "connection" */
    struct sockaddr request_addr; /* Who sent the most recent request */
    socklen_t request_addr_size;
    unsigned char *hdrbuf; /* udp packet headers */
    int    hdrsize;   /* number of headers' worth of space is allocated */
    conn   *next;     /* Used for generating a list of conn structures */
    LIBEVENT_THREAD *thread; /* Pointer to the thread object serving this connection */
};

/* array of conn structures, indexed by file descriptor */
extern conn **conns;

/* conn management */
conn *conn_new(const int sfd, const int init_state, const int event_flags, const int read_buffer_size, const bool is_udp, struct event_base *base);
void conn_set_state(conn *c, int state);

void conn_worker_readd(conn *c);
extern int daemonize(int nochdir, int noclose);

/*
 * In multithreaded mode, we wrap certain functions with lock management and
 * replace the logic of some other functions. All wrapped functions have
 * "mt_" and "do_" variants. In multithreaded mode, the plain version of a
 * function is #define-d to the "mt_" variant, which often just grabs a
 * lock and calls the "do_" function. In singlethreaded mode, the "do_"
 * function is called directly.
 *
 * Functions such as the libevent-related calls that need to do cross-thread
 * communication in multithreaded mode (rather than actually doing the work
 * in the current thread) are called via "dispatch_" frontends, which are
 * also #define-d to directly call the underlying code in singlethreaded mode.
 */

void thread_init(int nthreads, struct event_base *main_base);
int  dispatch_event_add(int thread, conn *c);
void dispatch_conn_new(int sfd, int init_state, int event_flags, int read_buffer_size, int is_udp);
void sidethread_conn_close(conn *c);

/* Lock wrappers for cache functions that are called from main loop. */
int   mt_is_listen_thread(void);
void  mt_stats_lock(void);
void  mt_stats_unlock(void);

# define is_listen_thread()          mt_is_listen_thread()

# define STATS_LOCK()                mt_stats_lock()
# define STATS_UNLOCK()              mt_stats_unlock()

extern int daemon_quit;

extern PQM g_pqm;

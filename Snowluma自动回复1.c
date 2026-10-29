/*
 * Snowluma (OneBot v11) 群消息自动回复程序 —— C 语言版（零第三方依赖）
 *
 * 功能：连接 Snowluma 正向 WebSocket，监听指定群聊，
 *       当群内消息中包含 "扣1" 时，立即回复 "1"，
 *       之后该群进入 1 分钟冷却，冷却期间不再触发。
 *
 * 实现：标准 C99 + 系统 socket（Windows: Winsock2 / Linux: POSIX），
 *       手写 WebSocket 客户端协议 + 精简 JSON 解析器，不需要任何外部库。
 *
 * 编译：
 *   Windows (MSVC)   cl /utf-8 /O2 Snowluma自动回复1.c ws2_32.lib
 *   Windows (MinGW)  gcc -O2 -o Snowluma自动回复1.exe Snowluma自动回复1.c -lws2_32
 *   Linux            gcc -O2 -o snowluma Snowluma自动回复1.c
 *
 * 注意：源文件必须保存为 UTF-8 编码，否则中文日志会乱码。
 */

#define _CRT_SECURE_NO_WARNINGS

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdarg.h>
#include <time.h>
#include <signal.h>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#if defined(_MSC_VER)
#pragma execution_character_set("utf-8")
#pragma comment(lib, "ws2_32.lib")
#endif
typedef SOCKET sock_t;
typedef int socklen_t_c;
#define SOCK_INVALID      INVALID_SOCKET
#define sock_close        closesocket
#define SOCK_WOULDBLOCK() (WSAGetLastError() == WSAEWOULDBLOCK)
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
typedef int sock_t;
typedef socklen_t socklen_t_c;
#define SOCK_INVALID      (-1)
#define sock_close        close
#define SOCK_WOULDBLOCK() (errno == EINPROGRESS)
#endif

/* ---------------- 配置 ---------------- */

static const char *WS_HOST = "127.0.0.1";
#define WS_PORT        3001      /* Snowluma 正向 WebSocket 默认端口 */
#define COOLDOWN       60        /* 冷却时间：1 分钟（秒） */
#define RETRY_DELAY    3         /* 断线重连间隔（秒） */
#define CONNECT_TIMEOUT_MS 5000

/* ---------------- 小工具 ---------------- */

static void msleep(int ms)
{
#ifdef _WIN32
    Sleep((DWORD)ms);
#else
    struct timespec ts;
    ts.tv_sec  = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
#endif
}

/* 单调时钟，单位秒（用于冷却计时） */
static double now_seconds(void)
{
#ifdef _WIN32
    return (double)GetTickCount64() / 1000.0;
#else
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
#endif
}

/* xorshift 伪随机，用于 WebSocket 掩码与握手 Key（非加密用途） */
static uint64_t g_rng_state = 0;

static void rng_seed(void)
{
    uint64_t s = (uint64_t)time(NULL);
    s ^= (uint64_t)(uintptr_t)&g_rng_state;
#ifdef _WIN32
    s ^= (uint64_t)GetCurrentProcessId() << 32;
    s ^= (uint64_t)GetTickCount64();
#else
    s ^= (uint64_t)getpid() << 32;
#endif
    if (s == 0) s = 0x9E3779B97F4A7C15ULL;
    g_rng_state = s;
}

static uint32_t rng_next(void)
{
    uint64_t x = g_rng_state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    g_rng_state = x;
    return (uint32_t)(x >> 32);
}

static void rng_bytes(unsigned char *dst, size_t n)
{
    size_t i;
    for (i = 0; i < n; i++) dst[i] = (unsigned char)(rng_next() & 0xFF);
}

/* 在内存块中查找子串，找不到返回 NULL */
static void *mem_find(const void *hay, size_t hay_len, const void *needle, size_t needle_len)
{
    const unsigned char *h = (const unsigned char *)hay;
    const unsigned char *n = (const unsigned char *)needle;
    size_t i;
    if (needle_len == 0) return (void *)h;
    if (hay_len < needle_len) return NULL;
    for (i = 0; i + needle_len <= hay_len; i++) {
        if (h[i] == n[0] && memcmp(h + i, n, needle_len) == 0)
            return (void *)(h + i);
    }
    return NULL;
}

/* ---------------- 动态字节缓冲 ---------------- */

typedef struct {
    unsigned char *p;
    size_t len;
    size_t cap;
} buf_t;

static void buf_init(buf_t *b)
{
    b->p = NULL;
    b->len = 0;
    b->cap = 0;
}

static void buf_free(buf_t *b)
{
    free(b->p);
    b->p = NULL;
    b->len = 0;
    b->cap = 0;
}

static int buf_reserve(buf_t *b, size_t need)
{
    size_t cap;
    unsigned char *np;
    if (need <= b->cap) return 0;
    cap = b->cap ? b->cap : 256;
    while (cap < need) cap *= 2;
    np = (unsigned char *)realloc(b->p, cap);
    if (!np) return -1;
    b->p = np;
    b->cap = cap;
    return 0;
}

static int buf_append(buf_t *b, const void *data, size_t n)
{
    if (n == 0) return 0;
    if (buf_reserve(b, b->len + n) != 0) return -1;
    memcpy(b->p + b->len, data, n);
    b->len += n;
    return 0;
}

/* 追加一个字节（含结尾 '\0' 时用于生成 C 字符串） */
static int buf_push(buf_t *b, unsigned char c)
{
    if (buf_reserve(b, b->len + 1) != 0) return -1;
    b->p[b->len++] = c;
    return 0;
}

/* ---------------- Base64 ---------------- */

static void base64_encode(const unsigned char *in, size_t in_len, char *out)
{
    static const char tbl[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    size_t i;
    char *o = out;
    for (i = 0; i + 2 < in_len; i += 3) {
        uint32_t v = ((uint32_t)in[i] << 16) | ((uint32_t)in[i + 1] << 8) | in[i + 2];
        *o++ = tbl[(v >> 18) & 0x3F];
        *o++ = tbl[(v >> 12) & 0x3F];
        *o++ = tbl[(v >> 6) & 0x3F];
        *o++ = tbl[v & 0x3F];
    }
    if (i < in_len) {
        uint32_t v = (uint32_t)in[i] << 16;
        if (i + 1 < in_len) v |= (uint32_t)in[i + 1] << 8;
        *o++ = tbl[(v >> 18) & 0x3F];
        *o++ = tbl[(v >> 12) & 0x3F];
        *o++ = (i + 1 < in_len) ? tbl[(v >> 6) & 0x3F] : '=';
        *o++ = '=';
    }
    *o = '\0';
}

/* ---------------- TCP 连接（带超时） ---------------- */

#ifdef _WIN32
static void set_nonblocking(sock_t s, int on)
{
    u_long mode = on ? 1UL : 0UL;
    ioctlsocket(s, FIONBIO, &mode);
}
#else
static void set_nonblocking(sock_t s, int on)
{
    int fl = fcntl(s, F_GETFL, 0);
    if (fl < 0) return;
    fl = on ? (fl | O_NONBLOCK) : (fl & ~O_NONBLOCK);
    fcntl(s, F_SETFL, fl);
}
#endif

static int socket_has_error(sock_t s)
{
    int err = 0;
    socklen_t_c len = (socklen_t_c)sizeof(err);
    if (getsockopt(s, SOL_SOCKET, SO_ERROR, (char *)&err, &len) != 0) return 1;
    return err != 0;
}

static sock_t tcp_connect(const char *host, int port, int timeout_ms)
{
    struct addrinfo hints, *res = NULL, *p;
    char portstr[16];
    sock_t s = SOCK_INVALID;

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    snprintf(portstr, sizeof(portstr), "%d", port);

    if (getaddrinfo(host, portstr, &hints, &res) != 0) return SOCK_INVALID;

    for (p = res; p != NULL; p = p->ai_next) {
        int rc;
        s = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (s == SOCK_INVALID) continue;

        set_nonblocking(s, 1);
        rc = connect(s, p->ai_addr, (int)p->ai_addrlen);
        if (rc == 0) {
            set_nonblocking(s, 0);
            break;
        }
        if (SOCK_WOULDBLOCK()) {
            fd_set wf;
            struct timeval tv;
            int sel;
            FD_ZERO(&wf);
            FD_SET(s, &wf);
            tv.tv_sec = timeout_ms / 1000;
            tv.tv_usec = (timeout_ms % 1000) * 1000;
            sel = select((int)s + 1, NULL, &wf, NULL, &tv);
            if (sel > 0 && !socket_has_error(s)) {
                set_nonblocking(s, 0);
                break;
            }
        }
        sock_close(s);
        s = SOCK_INVALID;
    }

    freeaddrinfo(res);
    return s;
}

static int send_all(sock_t s, const unsigned char *data, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        int n = send(s, (const char *)data + sent, (int)(len - sent), 0);
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

/* ---------------- WebSocket 客户端 ---------------- */

typedef struct {
    sock_t sock;
    buf_t in;          /* 已从 socket 读入但尚未消费的数据 */
} ws_t;

/* 精确读取 n 字节 */
static int ws_read_exact(ws_t *ws, unsigned char *dst, size_t n)
{
    while (ws->in.len < n) {
        unsigned char tmp[65536];
        int r = recv(ws->sock, (char *)tmp, (int)sizeof(tmp), 0);
        if (r <= 0) return -1;
        if (buf_append(&ws->in, tmp, (size_t)r) != 0) return -1;
    }
    memcpy(dst, ws->in.p, n);
    if (ws->in.len > n)
        memmove(ws->in.p, ws->in.p + n, ws->in.len - n);
    ws->in.len -= n;
    return 0;
}

static int ws_send_frame(ws_t *ws, unsigned char opcode, const unsigned char *payload, size_t len)
{
    unsigned char hdr[14];
    size_t hl = 0;
    unsigned char mask[4];
    buf_t frame;
    size_t i;
    int rc;

    hdr[hl++] = (unsigned char)(0x80 | (opcode & 0x0F));   /* FIN + opcode */

    if (len < 126) {
        hdr[hl++] = (unsigned char)(0x80 | len);
    } else if (len < 65536) {
        hdr[hl++] = 0x80 | 126;
        hdr[hl++] = (unsigned char)((len >> 8) & 0xFF);
        hdr[hl++] = (unsigned char)(len & 0xFF);
    } else {
        int k;
        hdr[hl++] = 0x80 | 127;
        for (k = 7; k >= 0; k--)
            hdr[hl++] = (unsigned char)((len >> (k * 8)) & 0xFF);
    }

    rng_bytes(mask, sizeof(mask));
    memcpy(hdr + hl, mask, 4);
    hl += 4;

    buf_init(&frame);
    if (buf_append(&frame, hdr, hl) != 0 ||
        buf_reserve(&frame, hl + len) != 0) {
        buf_free(&frame);
        return -1;
    }
    for (i = 0; i < len; i++)
        frame.p[hl + i] = (unsigned char)(payload[i] ^ mask[i % 4]);
    frame.len = hl + len;

    rc = send_all(ws->sock, frame.p, frame.len);
    buf_free(&frame);
    return rc;
}

static int ws_send_text(ws_t *ws, const char *text)
{
    return ws_send_frame(ws, 0x1, (const unsigned char *)text, strlen(text));
}

/* 建立连接并完成握手；失败时返回 -1 并写入 err */
static int ws_connect(ws_t *ws, const char *host, int port, const char *path,
                      char *err, size_t err_len)
{
    unsigned char raw[16];
    char key[32];
    char req[512];
    int rl;
    size_t hdr_end = 0;
    int found;

    memset(ws, 0, sizeof(*ws));
    buf_init(&ws->in);

    ws->sock = tcp_connect(host, port, CONNECT_TIMEOUT_MS);
    if (ws->sock == SOCK_INVALID) {
        snprintf(err, err_len, "无法连接 %s:%d", host, port);
        return -1;
    }

    rng_bytes(raw, sizeof(raw));
    base64_encode(raw, sizeof(raw), key);

    rl = snprintf(req, sizeof(req),
                  "GET %s HTTP/1.1\r\n"
                  "Host: %s:%d\r\n"
                  "Upgrade: websocket\r\n"
                  "Connection: Upgrade\r\n"
                  "Sec-WebSocket-Key: %s\r\n"
                  "Sec-WebSocket-Version: 13\r\n"
                  "\r\n",
                  path, host, port, key);
    if (rl <= 0 || (size_t)rl >= sizeof(req) ||
        send_all(ws->sock, (const unsigned char *)req, (size_t)rl) != 0) {
        snprintf(err, err_len, "发送握手请求失败");
        goto fail;
    }

    /* 读取 HTTP 响应头，直到遇到空行 */
    for (;;) {
        found = 0;
        if (ws->in.len >= 4) {
            size_t i;
            for (i = 0; i + 4 <= ws->in.len; i++) {
                if (memcmp(ws->in.p + i, "\r\n\r\n", 4) == 0) {
                    hdr_end = i + 4;
                    found = 1;
                    break;
                }
            }
        }
        if (found) break;
        {
            unsigned char tmp[4096];
            int r = recv(ws->sock, (char *)tmp, (int)sizeof(tmp), 0);
            if (r <= 0) {
                snprintf(err, err_len, "握手失败：连接被关闭");
                goto fail;
            }
            if (buf_append(&ws->in, tmp, (size_t)r) != 0) {
                snprintf(err, err_len, "内存不足");
                goto fail;
            }
        }
    }

    /* 检查状态码是否为 101 */
    {
        char head[180];
        size_t n = hdr_end < sizeof(head) - 1 ? hdr_end : sizeof(head) - 1;
        memcpy(head, ws->in.p, n);
        head[n] = '\0';
        if (!mem_find(head, n, " 101", 4)) {
            char *eol = strstr(head, "\r\n");
            if (eol) *eol = '\0';
            snprintf(err, err_len, "握手被拒绝: %s", head);
            goto fail;
        }
    }

    /* 丢弃响应头，保留其后可能已到达的帧数据 */
    if (ws->in.len > hdr_end)
        memmove(ws->in.p, ws->in.p + hdr_end, ws->in.len - hdr_end);
    ws->in.len -= hdr_end;
    return 0;

fail:
    sock_close(ws->sock);
    ws->sock = SOCK_INVALID;
    buf_free(&ws->in);
    return -1;
}

static void ws_close(ws_t *ws)
{
    if (ws->sock != SOCK_INVALID) {
        sock_close(ws->sock);
        ws->sock = SOCK_INVALID;
    }
    buf_free(&ws->in);
}

/*
 * 接收一条完整的文本消息（自动回应 ping、忽略 pong）。
 * 返回 0 成功；-1 表示连接关闭或出错。
 */
static int ws_recv_message(ws_t *ws, buf_t *out)
{
    buf_init(out);

    for (;;) {
        unsigned char h[2];
        int fin, opcode, masked;
        uint64_t len;
        unsigned char mask[4];
        unsigned char *payload;

        if (ws_read_exact(ws, h, 2) != 0) return -1;

        fin    = (h[0] & 0x80) != 0;
        opcode = h[0] & 0x0F;
        masked = (h[1] & 0x80) != 0;
        len    = (uint64_t)(h[1] & 0x7F);

        if (len == 126) {
            unsigned char ext[2];
            if (ws_read_exact(ws, ext, 2) != 0) return -1;
            len = ((uint64_t)ext[0] << 8) | ext[1];
        } else if (len == 127) {
            unsigned char ext[8];
            int k;
            if (ws_read_exact(ws, ext, 8) != 0) return -1;
            len = 0;
            for (k = 0; k < 8; k++) len = (len << 8) | ext[k];
        }

        if (len > 64u * 1024u * 1024u) return -1;   /* 防御异常长度 */

        if (masked && ws_read_exact(ws, mask, 4) != 0) return -1;

        payload = (unsigned char *)malloc((size_t)len + 1);
        if (!payload) return -1;
        if (len > 0 && ws_read_exact(ws, payload, (size_t)len) != 0) {
            free(payload);
            return -1;
        }
        payload[len] = 0;

        if (masked) {
            size_t i;
            for (i = 0; i < (size_t)len; i++)
                payload[i] ^= mask[i % 4];
        }

        if (opcode == 0x8) {                 /* close */
            free(payload);
            return -1;
        }
        if (opcode == 0x9) {                 /* ping -> pong */
            ws_send_frame(ws, 0xA, payload, (size_t)len);
            free(payload);
            continue;
        }
        if (opcode == 0xA) {                 /* pong */
            free(payload);
            continue;
        }
        if (opcode == 0x1 || opcode == 0x0) { /* text / continuation */
            if (buf_append(out, payload, (size_t)len) != 0) {
                free(payload);
                return -1;
            }
            free(payload);
            if (fin) return 0;
            continue;                         /* 分片未结束，继续读 */
        }
        free(payload);                        /* 其他类型忽略 */
    }
}

/* ---------------- 精简 JSON 解析器 ---------------- */

typedef enum {
    JSON_NULL,
    JSON_BOOL,
    JSON_NUMBER,
    JSON_STRING,
    JSON_ARRAY,
    JSON_OBJECT
} json_type;

typedef struct json_value {
    json_type type;
    int boolean;
    double number;
    int is_int;
    long long inum;
    char *string;          /* UTF-8，已转义解码 */
    size_t string_len;
    char *key;             /* 作为对象成员时的键 */
    size_t key_len;
    struct json_value *child;
    struct json_value *next;
} json_value;

typedef struct {
    const char *s;
    size_t len;
    size_t pos;
} json_parser;

static void json_free(json_value *v);

static void json_skip_ws(json_parser *p)
{
    while (p->pos < p->len) {
        char c = p->s[p->pos];
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') p->pos++;
        else break;
    }
}

static int json_read_hex4(json_parser *p, unsigned *out)
{
    unsigned v = 0;
    int i;
    if (p->pos + 4 > p->len) return -1;
    for (i = 0; i < 4; i++) {
        char c = p->s[p->pos++];
        v <<= 4;
        if (c >= '0' && c <= '9') v |= (unsigned)(c - '0');
        else if (c >= 'a' && c <= 'f') v |= (unsigned)(c - 'a' + 10);
        else if (c >= 'A' && c <= 'F') v |= (unsigned)(c - 'A' + 10);
        else return -1;
    }
    *out = v;
    return 0;
}

static void utf8_push(buf_t *b, unsigned cp)
{
    if (cp <= 0x7F) {
        buf_push(b, (unsigned char)cp);
    } else if (cp <= 0x7FF) {
        buf_push(b, (unsigned char)(0xC0 | (cp >> 6)));
        buf_push(b, (unsigned char)(0x80 | (cp & 0x3F)));
    } else if (cp <= 0xFFFF) {
        buf_push(b, (unsigned char)(0xE0 | (cp >> 12)));
        buf_push(b, (unsigned char)(0x80 | ((cp >> 6) & 0x3F)));
        buf_push(b, (unsigned char)(0x80 | (cp & 0x3F)));
    } else {
        buf_push(b, (unsigned char)(0xF0 | (cp >> 18)));
        buf_push(b, (unsigned char)(0x80 | ((cp >> 12) & 0x3F)));
        buf_push(b, (unsigned char)(0x80 | ((cp >> 6) & 0x3F)));
        buf_push(b, (unsigned char)(0x80 | (cp & 0x3F)));
    }
}

/* 解析字符串（当前位置必须是 '"'），输出 malloc 的 UTF-8 字符串 */
static int json_parse_string(json_parser *p, char **out, size_t *out_len)
{
    buf_t b;

    if (p->pos >= p->len || p->s[p->pos] != '"') return -1;
    p->pos++;

    buf_init(&b);

    while (p->pos < p->len) {
        unsigned char c = (unsigned char)p->s[p->pos++];

        if (c == '"') {
            if (buf_push(&b, 0) != 0) { buf_free(&b); return -1; }
            *out = (char *)b.p;
            *out_len = b.len - 1;
            return 0;
        }

        if (c == '\\') {
            char e;
            if (p->pos >= p->len) { buf_free(&b); return -1; }
            e = p->s[p->pos++];
            switch (e) {
            case '"':  buf_push(&b, '"');  break;
            case '\\': buf_push(&b, '\\'); break;
            case '/':  buf_push(&b, '/');  break;
            case 'b':  buf_push(&b, '\b'); break;
            case 'f':  buf_push(&b, '\f'); break;
            case 'n':  buf_push(&b, '\n'); break;
            case 'r':  buf_push(&b, '\r'); break;
            case 't':  buf_push(&b, '\t'); break;
            case 'u': {
                unsigned cp;
                if (json_read_hex4(p, &cp) != 0) { buf_free(&b); return -1; }
                if (cp >= 0xD800 && cp <= 0xDBFF) {
                    size_t save = p->pos;
                    unsigned lo;
                    if (p->pos + 1 < p->len && p->s[p->pos] == '\\' &&
                        p->s[p->pos + 1] == 'u') {
                        p->pos += 2;
                        if (json_read_hex4(p, &lo) == 0 && lo >= 0xDC00 && lo <= 0xDFFF) {
                            cp = 0x10000u + ((cp - 0xD800u) << 10) + (lo - 0xDC00u);
                        } else {
                            p->pos = save;
                        }
                    }
                }
                utf8_push(&b, cp);
                break;
            }
            default:
                buf_push(&b, (unsigned char)e);
                break;
            }
            continue;
        }

        buf_push(&b, c);
    }

    buf_free(&b);
    return -1;
}

static json_value *json_parse_value(json_parser *p, int depth);

static json_value *json_parse_object(json_parser *p, int depth)
{
    json_value *v = (json_value *)calloc(1, sizeof(*v));
    json_value **tail;
    if (!v) return NULL;
    v->type = JSON_OBJECT;
    p->pos++;   /* '{' */

    json_skip_ws(p);
    if (p->pos < p->len && p->s[p->pos] == '}') { p->pos++; return v; }

    tail = &v->child;
    for (;;) {
        char *key = NULL;
        size_t key_len = 0;
        json_value *child;

        json_skip_ws(p);
        if (json_parse_string(p, &key, &key_len) != 0) goto fail;

        json_skip_ws(p);
        if (p->pos >= p->len || p->s[p->pos] != ':') { free(key); goto fail; }
        p->pos++;

        child = json_parse_value(p, depth + 1);
        if (!child) { free(key); goto fail; }
        child->key = key;
        child->key_len = key_len;
        *tail = child;
        tail = &child->next;

        json_skip_ws(p);
        if (p->pos < p->len && p->s[p->pos] == ',') { p->pos++; continue; }
        if (p->pos < p->len && p->s[p->pos] == '}') { p->pos++; return v; }
        goto fail;
    }

fail:
    json_free(v);
    return NULL;
}

static json_value *json_parse_array(json_parser *p, int depth)
{
    json_value *v = (json_value *)calloc(1, sizeof(*v));
    json_value **tail;
    if (!v) return NULL;
    v->type = JSON_ARRAY;
    p->pos++;   /* '[' */

    json_skip_ws(p);
    if (p->pos < p->len && p->s[p->pos] == ']') { p->pos++; return v; }

    tail = &v->child;
    for (;;) {
        json_value *child = json_parse_value(p, depth + 1);
        if (!child) goto fail;
        *tail = child;
        tail = &child->next;

        json_skip_ws(p);
        if (p->pos < p->len && p->s[p->pos] == ',') { p->pos++; continue; }
        if (p->pos < p->len && p->s[p->pos] == ']') { p->pos++; return v; }
        goto fail;
    }

fail:
    json_free(v);
    return NULL;
}

static json_value *json_parse_value(json_parser *p, int depth)
{
    json_value *v;
    char c;

    if (depth > 64) return NULL;
    json_skip_ws(p);
    if (p->pos >= p->len) return NULL;
    c = p->s[p->pos];

    if (c == '{') return json_parse_object(p, depth);
    if (c == '[') return json_parse_array(p, depth);

    v = (json_value *)calloc(1, sizeof(*v));
    if (!v) return NULL;

    if (c == '"') {
        v->type = JSON_STRING;
        if (json_parse_string(p, &v->string, &v->string_len) != 0) { free(v); return NULL; }
        return v;
    }

    if (c == '-' || (c >= '0' && c <= '9')) {
        size_t start = p->pos;
        size_t n;
        char tmp[64];
        int is_int = 1;

        while (p->pos < p->len) {
            char d = p->s[p->pos];
            if ((d >= '0' && d <= '9') || d == '-' || d == '+' || d == '.' ||
                d == 'e' || d == 'E') {
                if (d == '.' || d == 'e' || d == 'E') is_int = 0;
                p->pos++;
            } else {
                break;
            }
        }
        n = p->pos - start;
        if (n == 0 || n >= sizeof(tmp)) { free(v); return NULL; }
        memcpy(tmp, p->s + start, n);
        tmp[n] = '\0';

        v->type = JSON_NUMBER;
        v->is_int = is_int;
        v->number = strtod(tmp, NULL);
        v->inum = is_int ? strtoll(tmp, NULL, 10) : (long long)v->number;
        return v;
    }

    if (p->pos + 4 <= p->len && memcmp(p->s + p->pos, "true", 4) == 0) {
        v->type = JSON_BOOL;
        v->boolean = 1;
        p->pos += 4;
        return v;
    }
    if (p->pos + 5 <= p->len && memcmp(p->s + p->pos, "false", 5) == 0) {
        v->type = JSON_BOOL;
        v->boolean = 0;
        p->pos += 5;
        return v;
    }
    if (p->pos + 4 <= p->len && memcmp(p->s + p->pos, "null", 4) == 0) {
        v->type = JSON_NULL;
        p->pos += 4;
        return v;
    }

    free(v);
    return NULL;
}

static json_value *json_parse(const char *text, size_t len)
{
    json_parser p;
    p.s = text;
    p.len = len;
    p.pos = 0;
    return json_parse_value(&p, 0);
}

static void json_free(json_value *v)
{
    while (v) {
        json_value *next = v->next;
        json_free(v->child);
        free(v->string);
        free(v->key);
        free(v);
        v = next;
    }
}

static json_value *json_obj_get(const json_value *obj, const char *key)
{
    json_value *c;
    size_t klen;
    if (!obj || obj->type != JSON_OBJECT) return NULL;
    klen = strlen(key);
    for (c = obj->child; c; c = c->next) {
        if (c->key && c->key_len == klen && memcmp(c->key, key, klen) == 0)
            return c;
    }
    return NULL;
}

static const char *json_as_string(const json_value *v)
{
    if (v && v->type == JSON_STRING) return v->string;
    return NULL;
}

static int json_as_ll(const json_value *v, long long *out)
{
    if (!v || v->type != JSON_NUMBER) return -1;
    *out = v->inum;
    return 0;
}

/* ---------------- 日志与界面 ---------------- */

static void logmsg(const char *fmt, ...)
{
    char ts[16];
    time_t t = time(NULL);
    struct tm tmv;
    va_list ap;

#ifdef _WIN32
    localtime_s(&tmv, &t);
#else
    localtime_r(&t, &tmv);
#endif
    strftime(ts, sizeof(ts), "%H:%M:%S", &tmv);

    printf("[%s] ", ts);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    fflush(stdout);
}

static void banner(void)
{
    printf("==================================================\n");
    printf("  Snowluma 群消息自动回复工具（C 语言版）\n");
    printf("==================================================\n");
    printf("  功能说明：\n");
    printf("   - 自动连接本机 Snowluma (正向 WebSocket)\n");
    printf("   - 监听指定群聊消息\n");
    printf("   - 消息中包含 \"扣1\" 时立即回复 \"1\"\n");
    printf("   - 触发后全群冷却 1 分钟\n");
    printf("   - 断线自动重连\n");
    printf("  连接地址：ws://%s:%d\n", WS_HOST, WS_PORT);
    printf("==================================================\n");
}

/* ---------------- 主逻辑 ---------------- */

static const char KEYWORD[] = "扣1";     /* UTF-8 字节序列 */

static void handle_event(ws_t *ws, const json_value *root, long long group_id,
                         double *last_reply)
{
    const json_value *v;
    const char *post_type;
    const char *message_type;
    const char *raw;
    size_t raw_len;
    long long gid;
    double now, remain;

    if (!root || root->type != JSON_OBJECT) return;

    post_type = json_as_string(json_obj_get(root, "post_type"));
    if (!post_type || strcmp(post_type, "message") != 0) return;

    message_type = json_as_string(json_obj_get(root, "message_type"));
    if (!message_type || strcmp(message_type, "group") != 0) return;

    if (json_as_ll(json_obj_get(root, "group_id"), &gid) != 0 || gid != group_id) return;

    v = json_obj_get(root, "raw_message");
    raw = json_as_string(v);
    if (!raw) return;
    raw_len = v->string_len;

    if (!mem_find(raw, raw_len, KEYWORD, sizeof(KEYWORD) - 1)) return;

    now = now_seconds();
    remain = (double)COOLDOWN - (now - *last_reply);

    {
        char user[64];
        const json_value *u = json_obj_get(root, "user_id");
        if (u && u->type == JSON_STRING) {
            snprintf(user, sizeof(user), "%s", u->string);
        } else if (u && u->type == JSON_NUMBER) {
            snprintf(user, sizeof(user), "%lld", u->inum);
        } else {
            snprintf(user, sizeof(user), "?");
        }

        if (remain > 0) {
            logmsg("群成员 %s 消息含 \"扣1\"，冷却中（剩余 %d 秒），忽略",
                   user, (int)remain);
            return;
        }

        /* 立即回复，保证最快响应 */
        {
            char payload[256];
            snprintf(payload, sizeof(payload),
                     "{\"action\":\"send_group_msg\","
                     "\"params\":{\"group_id\":%lld,\"message\":\"1\"}}",
                     group_id);
            if (ws_send_text(ws, payload) != 0) {
                logmsg("发送回复失败，连接可能已断开");
                return;
            }
        }

        *last_reply = now;
        logmsg("群成员 %s 消息含 \"扣1\"，已回复 \"1\"，进入 1 分钟冷却", user);
    }
}

static void run_loop(long long group_id)
{
    double last_reply = 0.0;

    for (;;) {
        ws_t ws;
        char err[256];

        logmsg("正在连接 ws://%s:%d ...", WS_HOST, WS_PORT);

        if (ws_connect(&ws, WS_HOST, WS_PORT, "/", err, sizeof(err)) != 0) {
            logmsg("%s，%d 秒后重连...", err, RETRY_DELAY);
            msleep(RETRY_DELAY * 1000);
            continue;
        }

        logmsg("已连接 Snowluma，开始监听群 %lld", group_id);

        for (;;) {
            buf_t msg;
            json_value *root;

            if (ws_recv_message(&ws, &msg) != 0) {
                buf_free(&msg);
                logmsg("连接被断开，%d 秒后重连...", RETRY_DELAY);
                break;
            }

            root = json_parse((const char *)msg.p, msg.len);
            buf_free(&msg);
            if (!root) continue;   /* 非 JSON 数据，忽略 */

            handle_event(&ws, root, group_id, &last_reply);
            json_free(root);
        }

        ws_close(&ws);
        msleep(RETRY_DELAY * 1000);
    }
}

/* ---------------- 输入与入口 ---------------- */

static int parse_group_id(const char *line, long long *out)
{
    const char *s = line;
    char buf[64];
    size_t n;
    size_t i;

    while (*s == ' ' || *s == '\t') s++;
    n = strlen(s);
    while (n > 0 && (s[n - 1] == '\n' || s[n - 1] == '\r' ||
                     s[n - 1] == ' '  || s[n - 1] == '\t')) n--;
    if (n == 0 || n >= sizeof(buf)) return 0;
    for (i = 0; i < n; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
    }
    memcpy(buf, s, n);
    buf[n] = '\0';
    *out = strtoll(buf, NULL, 10);
    return 1;
}

#ifdef _WIN32
static BOOL WINAPI on_ctrl(DWORD type)
{
    if (type == CTRL_C_EVENT || type == CTRL_BREAK_EVENT) {
        printf("\n已退出。\n");
        fflush(stdout);
        exit(0);
    }
    return FALSE;
}
#else
static void on_sigint(int sig)
{
    (void)sig;
    printf("\n已退出。\n");
    fflush(stdout);
    exit(0);
}
#endif

int main(void)
{
    long long group_id = 0;
    char line[128];

#ifdef _WIN32
    {
        WSADATA wsa;
        SetConsoleOutputCP(CP_UTF8);
        SetConsoleCP(CP_UTF8);
        if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
            printf("WSAStartup 失败，无法初始化网络。\n");
            return 1;
        }
        SetConsoleCtrlHandler(on_ctrl, TRUE);
    }
#else
    signal(SIGINT, on_sigint);
    signal(SIGPIPE, SIG_IGN);
#endif

    rng_seed();
    banner();

    for (;;) {
        printf("\n请输入要监听的群号: ");
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;   /* EOF */
        if (parse_group_id(line, &group_id)) break;
        printf("群号必须是纯数字，请重新输入。\n");
    }

    printf("\n已设定监听群号：%lld\n", group_id);
    printf("程序运行中，按 Ctrl+C 退出。\n\n");

    run_loop(group_id);

#ifdef _WIN32
    WSACleanup();
#endif
    return 0;
}

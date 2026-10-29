// ============================================================================
//  Snowluma 群消息自动回复工具 —— C++ 版
//
//  文件：Snowluma自动回复_cpp_3003.cpp
//  端口：3003（Snowluma 正向 WebSocket 监听端口）
//
//  功能说明：
//    - 自动连接本机 Snowluma (正向 WebSocket)  ws://127.0.0.1:3003
//    - 监听指定群聊消息
//    - 消息中包含 "扣1" 时立即回复 "1"
//    - 触发后全群冷却 1 分钟
//    - 断线自动重连
//
//  编译（MinGW-w64 / MSYS2，推荐）：
//    g++ -std=c++17 -O2 -o Snowluma自动回复_cpp_3003.exe Snowluma自动回复_cpp_3003.cpp -lws2_32
//
//  编译（MSVC）：
//    cl /std:c++17 /utf-8 /EHsc /O2 Snowluma自动回复_cpp_3003.cpp ws2_32.lib
// ============================================================================

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif

#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#if defined(_MSC_VER)
#pragma execution_character_set("utf-8")
#endif

// ============================================================================
//  可调参数
// ============================================================================
namespace cfg {

constexpr const char* kHost = "127.0.0.1";   // Snowluma 监听地址
constexpr int         kPort = 3003;          // Snowluma 监听端口（已改为 3003）

constexpr const char* kTrigger = "扣1";      // 触发关键词（UTF-8）
constexpr const char* kReply   = "1";        // 回复内容

constexpr double kCooldownSeconds  = 60.0;   // 触发后冷却时长（秒）
constexpr int    kReconnectDelayMs = 3000;   // 断线重连间隔（毫秒）
constexpr size_t kMaxFrameBytes    = 64u * 1024u * 1024u;  // 单帧上限 64MB

}  // namespace cfg

// ============================================================================
//  日志
// ============================================================================
static void logmsg(const char* fmt, ...) {
    char stamp[16] = {0};
    std::time_t now = std::time(nullptr);
    if (const std::tm* tm = std::localtime(&now)) {
        std::strftime(stamp, sizeof(stamp), "%H:%M:%S", tm);
    }

    std::printf("[%s] ", stamp);

    va_list ap;
    va_start(ap, fmt);
    std::vprintf(fmt, ap);
    va_end(ap);

    std::putchar('\n');
    std::fflush(stdout);
}

// ============================================================================
//  随机数 / Base64
// ============================================================================
namespace {

std::mt19937_64& rng() {
    static std::mt19937_64 engine = [] {
        std::random_device rd;
        std::seed_seq seq{
            rd(), rd(), rd(), rd(),
            static_cast<unsigned>(::GetCurrentProcessId()),
            static_cast<unsigned>(::GetTickCount64()),
        };
        return std::mt19937_64(seq);
    }();
    return engine;
}

void randomBytes(void* dst, size_t n) {
    auto* p = static_cast<unsigned char*>(dst);
    size_t i = 0;
    while (i < n) {
        const std::uint64_t v = rng()();
        for (size_t k = 0; k < 8 && i < n; ++k, ++i) {
            p[i] = static_cast<unsigned char>((v >> (k * 8)) & 0xFF);
        }
    }
}

std::string base64Encode(const unsigned char* data, size_t len) {
    static const char kTable[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    std::string out;
    out.reserve(((len + 2) / 3) * 4);

    size_t i = 0;
    for (; i + 2 < len; i += 3) {
        const unsigned v = (static_cast<unsigned>(data[i]) << 16) |
                           (static_cast<unsigned>(data[i + 1]) << 8) |
                           static_cast<unsigned>(data[i + 2]);
        out += kTable[(v >> 18) & 0x3F];
        out += kTable[(v >> 12) & 0x3F];
        out += kTable[(v >> 6) & 0x3F];
        out += kTable[v & 0x3F];
    }

    if (i < len) {
        const bool two = (i + 1 < len);
        unsigned v = static_cast<unsigned>(data[i]) << 16;
        if (two) v |= static_cast<unsigned>(data[i + 1]) << 8;
        out += kTable[(v >> 18) & 0x3F];
        out += kTable[(v >> 12) & 0x3F];
        out += two ? kTable[(v >> 6) & 0x3F] : '=';
        out += '=';
    }

    return out;
}

}  // namespace

// ============================================================================
//  极简 JSON 解析器
//  只需要读 OneBot 事件对象里的几个字段，因此实现保持精简：
//  支持 object / array / string / number / true / false / null，
//  数字区分整数与浮点，字符串支持 \uXXXX（含代理对）。
// ============================================================================
namespace json {

struct Value {
    enum class Type { Null, Bool, Number, String, Array, Object };

    Type        type     = Type::Null;
    bool        boolean  = false;
    long long   integer  = 0;
    double      number   = 0.0;
    bool        isInt    = false;
    std::string text;
    std::vector<Value> items;
    std::vector<std::pair<std::string, Value>> members;

    bool isString()  const { return type == Type::String; }
    bool isInteger() const { return type == Type::Number && isInt; }

    /// 取对象的某个成员，找不到返回 nullptr。
    const Value* find(std::string_view key) const {
        if (type != Type::Object) return nullptr;
        for (const auto& kv : members) {
            if (kv.first == key) return &kv.second;
        }
        return nullptr;
    }
};

namespace detail {

inline void appendUtf8(std::string& out, unsigned cp) {
    if (cp <= 0x7F) {
        out += static_cast<char>(cp);
    } else if (cp <= 0x7FF) {
        out += static_cast<char>(0xC0 | (cp >> 6));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp <= 0xFFFF) {
        out += static_cast<char>(0xE0 | (cp >> 12));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    } else {
        out += static_cast<char>(0xF0 | (cp >> 18));
        out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
        out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
        out += static_cast<char>(0x80 | (cp & 0x3F));
    }
}

class Parser {
public:
    explicit Parser(std::string_view src) : s_(src) {}

    std::optional<Value> parse() {
        auto v = parseValue(0);
        if (!v) return std::nullopt;
        return v;
    }

private:
    void skipWs() {
        while (i_ < s_.size()) {
            const char c = s_[i_];
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                ++i_;
            } else {
                break;
            }
        }
    }

    bool match(std::string_view lit) {
        if (s_.size() - i_ < lit.size()) return false;
        if (s_.compare(i_, lit.size(), lit) != 0) return false;
        i_ += lit.size();
        return true;
    }

    std::optional<Value> parseValue(int depth) {
        if (depth > 64) return std::nullopt;  // 防御畸形嵌套
        skipWs();
        if (i_ >= s_.size()) return std::nullopt;

        switch (s_[i_]) {
            case '{': return parseObject(depth);
            case '[': return parseArray(depth);
            case '"': {
                auto str = parseString();
                if (!str) return std::nullopt;
                Value v;
                v.type = Value::Type::String;
                v.text = std::move(*str);
                return v;
            }
            case 't':
                if (match("true")) {
                    Value v;
                    v.type = Value::Type::Bool;
                    v.boolean = true;
                    return v;
                }
                return std::nullopt;
            case 'f':
                if (match("false")) {
                    Value v;
                    v.type = Value::Type::Bool;
                    v.boolean = false;
                    return v;
                }
                return std::nullopt;
            case 'n':
                if (match("null")) return Value{};
                return std::nullopt;
            default: return parseNumber();
        }
    }

    std::optional<Value> parseObject(int depth) {
        ++i_;  // '{'
        Value obj;
        obj.type = Value::Type::Object;

        skipWs();
        if (i_ < s_.size() && s_[i_] == '}') {
            ++i_;
            return obj;
        }

        for (;;) {
            skipWs();
            if (i_ >= s_.size() || s_[i_] != '"') return std::nullopt;
            auto key = parseString();
            if (!key) return std::nullopt;

            skipWs();
            if (i_ >= s_.size() || s_[i_] != ':') return std::nullopt;
            ++i_;

            auto val = parseValue(depth + 1);
            if (!val) return std::nullopt;
            obj.members.emplace_back(std::move(*key), std::move(*val));

            skipWs();
            if (i_ >= s_.size()) return std::nullopt;
            if (s_[i_] == ',') { ++i_; continue; }
            if (s_[i_] == '}') { ++i_; return obj; }
            return std::nullopt;
        }
    }

    std::optional<Value> parseArray(int depth) {
        ++i_;  // '['
        Value arr;
        arr.type = Value::Type::Array;

        skipWs();
        if (i_ < s_.size() && s_[i_] == ']') {
            ++i_;
            return arr;
        }

        for (;;) {
            auto val = parseValue(depth + 1);
            if (!val) return std::nullopt;
            arr.items.push_back(std::move(*val));

            skipWs();
            if (i_ >= s_.size()) return std::nullopt;
            if (s_[i_] == ',') { ++i_; continue; }
            if (s_[i_] == ']') { ++i_; return arr; }
            return std::nullopt;
        }
    }

    std::optional<std::string> parseString() {
        ++i_;  // '"'
        std::string out;

        while (i_ < s_.size()) {
            const unsigned char c = static_cast<unsigned char>(s_[i_++]);
            if (c == '"') return out;

            if (c != '\\') {
                out += static_cast<char>(c);
                continue;
            }

            if (i_ >= s_.size()) return std::nullopt;
            const char esc = s_[i_++];
            switch (esc) {
                case '"':  out += '"';  break;
                case '\\': out += '\\'; break;
                case '/':  out += '/';  break;
                case 'b':  out += '\b'; break;
                case 'f':  out += '\f'; break;
                case 'n':  out += '\n'; break;
                case 'r':  out += '\r'; break;
                case 't':  out += '\t'; break;
                case 'u': {
                    unsigned cp = 0;
                    if (!readHex4(cp)) return std::nullopt;
                    // 代理对
                    if (cp >= 0xD800 && cp <= 0xDBFF && i_ + 1 < s_.size() &&
                        s_[i_] == '\\' && s_[i_ + 1] == 'u') {
                        i_ += 2;
                        unsigned low = 0;
                        if (!readHex4(low)) return std::nullopt;
                        if (low >= 0xDC00 && low <= 0xDFFF) {
                            cp = 0x10000 + ((cp - 0xD800) << 10) + (low - 0xDC00);
                        } else {
                            appendUtf8(out, cp);
                            cp = low;
                        }
                    }
                    appendUtf8(out, cp);
                    break;
                }
                default: return std::nullopt;
            }
        }
        return std::nullopt;
    }

    bool readHex4(unsigned& out) {
        if (s_.size() - i_ < 4) return false;
        unsigned v = 0;
        for (int k = 0; k < 4; ++k) {
            const char c = s_[i_++];
            unsigned d;
            if (c >= '0' && c <= '9')      d = static_cast<unsigned>(c - '0');
            else if (c >= 'a' && c <= 'f') d = static_cast<unsigned>(c - 'a' + 10);
            else if (c >= 'A' && c <= 'F') d = static_cast<unsigned>(c - 'A' + 10);
            else return false;
            v = (v << 4) | d;
        }
        out = v;
        return true;
    }

    std::optional<Value> parseNumber() {
        const size_t start = i_;
        bool isFloat = false;

        if (i_ < s_.size() && (s_[i_] == '-' || s_[i_] == '+')) ++i_;
        while (i_ < s_.size()) {
            const char c = s_[i_];
            if (c >= '0' && c <= '9') {
                ++i_;
            } else if (c == '.' || c == 'e' || c == 'E' || c == '+' || c == '-') {
                isFloat = isFloat || (c == '.' || c == 'e' || c == 'E');
                ++i_;
            } else {
                break;
            }
        }
        if (i_ == start) return std::nullopt;

        const std::string token(s_.substr(start, i_ - start));

        Value v;
        v.type = Value::Type::Number;
        if (isFloat) {
            v.number = std::strtod(token.c_str(), nullptr);
            v.integer = static_cast<long long>(v.number);
            v.isInt = false;
        } else {
            v.integer = std::strtoll(token.c_str(), nullptr, 10);
            v.number = static_cast<double>(v.integer);
            v.isInt = true;
        }
        return v;
    }

    std::string_view s_;
    size_t i_ = 0;
};

}  // namespace detail

inline std::optional<Value> parse(std::string_view text) {
    return detail::Parser(text).parse();
}

}  // namespace json

// ============================================================================
//  WebSocket 客户端（正向连接 Snowluma）
// ============================================================================
class WebSocket {
public:
    WebSocket() = default;
    ~WebSocket() { close(); }

    WebSocket(const WebSocket&) = delete;
    WebSocket& operator=(const WebSocket&) = delete;

    /// 建立 TCP 连接 + 完成 WebSocket 握手。失败时 err 带回原因。
    bool connectTo(const std::string& host, int port, std::string& err) {
        close();

        const std::string portStr = std::to_string(port);

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;

        addrinfo* res = nullptr;
        if (::getaddrinfo(host.c_str(), portStr.c_str(), &hints, &res) != 0) {
            err = "无法连接 " + host + ":" + portStr;
            return false;
        }

        SOCKET s = INVALID_SOCKET;
        for (addrinfo* ai = res; ai != nullptr; ai = ai->ai_next) {
            s = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
            if (s == INVALID_SOCKET) continue;

            // 非阻塞 connect + select 等待，超时 5 秒
            u_long nonBlocking = 1;
            ::ioctlsocket(s, FIONBIO, &nonBlocking);

            bool ok = false;
            if (::connect(s, ai->ai_addr, static_cast<int>(ai->ai_addrlen)) == 0) {
                ok = true;
            } else if (::WSAGetLastError() == WSAEWOULDBLOCK) {
                fd_set writeSet;
                fd_set errSet;
                FD_ZERO(&writeSet);
                FD_ZERO(&errSet);
                FD_SET(s, &writeSet);
                FD_SET(s, &errSet);

                timeval tv{};
                tv.tv_sec = 5;
                tv.tv_usec = 0;

                if (::select(0, nullptr, &writeSet, &errSet, &tv) > 0) {
                    int soError = 0;
                    int soLen = static_cast<int>(sizeof(soError));
                    if (::getsockopt(s, SOL_SOCKET, SO_ERROR,
                                     reinterpret_cast<char*>(&soError), &soLen) == 0 &&
                        soError == 0) {
                        ok = true;
                    }
                }
            }

            if (!ok) {
                ::closesocket(s);
                s = INVALID_SOCKET;
                continue;
            }

            u_long blocking = 0;
            ::ioctlsocket(s, FIONBIO, &blocking);
            break;
        }

        ::freeaddrinfo(res);

        if (s == INVALID_SOCKET) {
            err = "无法连接 " + host + ":" + portStr;
            return false;
        }
        sock_ = s;

        return handshake(host, port, err);
    }

    void close() {
        if (sock_ != INVALID_SOCKET) {
            ::closesocket(sock_);
            sock_ = INVALID_SOCKET;
        }
        rx_.clear();
    }

    /// 读取一个完整帧。返回 false 表示连接已断开或出错。
    bool readFrame(int& opcode, bool& fin, std::string& payload) {
        unsigned char hdr[2];
        if (!readExact(hdr, 2)) return false;

        fin = (hdr[0] & 0x80) != 0;
        opcode = hdr[0] & 0x0F;
        const bool masked = (hdr[1] & 0x80) != 0;
        std::uint64_t len = hdr[1] & 0x7F;

        if (len == 126) {
            unsigned char ext[2];
            if (!readExact(ext, 2)) return false;
            len = (static_cast<std::uint64_t>(ext[0]) << 8) | ext[1];
        } else if (len == 127) {
            unsigned char ext[8];
            if (!readExact(ext, 8)) return false;
            len = 0;
            for (int k = 0; k < 8; ++k) {
                len = (len << 8) | ext[k];
            }
            if (len > cfg::kMaxFrameBytes) return false;
        }

        unsigned char mask[4] = {0, 0, 0, 0};
        if (masked && !readExact(mask, 4)) return false;

        payload.assign(static_cast<size_t>(len), '\0');
        if (len != 0 && !readExact(payload.data(), static_cast<size_t>(len))) return false;

        if (masked) {
            for (size_t k = 0; k < payload.size(); ++k) {
                payload[k] = static_cast<char>(payload[k] ^ static_cast<char>(mask[k & 3]));
            }
        }
        return true;
    }

    /// 发送一帧（客户端必须加掩码）。
    bool sendFrame(int opcode, const void* data, size_t len) {
        std::vector<unsigned char> frame;
        frame.reserve(len + 14);

        frame.push_back(static_cast<unsigned char>(0x80 | (opcode & 0x0F)));  // FIN + opcode

        const unsigned char kMaskBit = 0x80;
        if (len <= 125) {
            frame.push_back(static_cast<unsigned char>(kMaskBit | len));
        } else if (len <= 0xFFFF) {
            frame.push_back(static_cast<unsigned char>(kMaskBit | 126));
            frame.push_back(static_cast<unsigned char>((len >> 8) & 0xFF));
            frame.push_back(static_cast<unsigned char>(len & 0xFF));
        } else {
            frame.push_back(static_cast<unsigned char>(kMaskBit | 127));
            for (int k = 7; k >= 0; --k) {
                frame.push_back(static_cast<unsigned char>((static_cast<std::uint64_t>(len) >> (k * 8)) & 0xFF));
            }
        }

        unsigned char mask[4];
        randomBytes(mask, sizeof(mask));
        frame.insert(frame.end(), mask, mask + 4);

        const auto* src = static_cast<const unsigned char*>(data);
        for (size_t k = 0; k < len; ++k) {
            frame.push_back(static_cast<unsigned char>(src[k] ^ mask[k & 3]));
        }

        return sendAll(frame.data(), frame.size());
    }

private:
    bool handshake(const std::string& host, int port, std::string& err) {
        unsigned char keyBytes[16];
        randomBytes(keyBytes, sizeof(keyBytes));
        const std::string key = base64Encode(keyBytes, sizeof(keyBytes));

        char request[512];
        const int n = std::snprintf(
            request, sizeof(request),
            "GET %s HTTP/1.1\r\n"
            "Host: %s:%d\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: %s\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n",
            "/", host.c_str(), port, key.c_str());

        if (n <= 0 || n >= static_cast<int>(sizeof(request))) {
            close();
            err = "内存不足";
            return false;
        }

        if (!sendAll(request, static_cast<size_t>(n))) {
            close();
            err = "发送握手请求失败";
            return false;
        }

        // 一直读到响应头结束标记 \r\n\r\n
        std::string head;
        char chunk[4096];
        while (head.find("\r\n\r\n") == std::string::npos) {
            const int r = ::recv(sock_, chunk, static_cast<int>(sizeof(chunk)), 0);
            if (r <= 0) {
                close();
                err = "握手失败：连接被关闭";
                return false;
            }
            head.append(chunk, static_cast<size_t>(r));
        }

        if (head.find(" 101") == std::string::npos) {
            std::string brief = head.substr(0, 200);
            for (char& c : brief) {
                if (c == '\r' || c == '\n') c = ' ';
            }
            close();
            err = "握手被拒绝: " + brief;
            return false;
        }

        // 响应头之后多读到的字节属于后续帧，留在缓冲里
        const size_t headEnd = head.find("\r\n\r\n") + 4;
        rx_ = head.substr(headEnd);
        return true;
    }

    bool readExact(void* buf, size_t n) {
        if (n == 0) return true;

        if (rx_.size() < n) {
            char chunk[65536];
            while (rx_.size() < n) {
                const int r = ::recv(sock_, chunk, static_cast<int>(sizeof(chunk)), 0);
                if (r <= 0) return false;
                rx_.append(chunk, static_cast<size_t>(r));
            }
        }

        std::memcpy(buf, rx_.data(), n);
        rx_.erase(0, n);
        return true;
    }

    bool sendAll(const void* data, size_t n) {
        const auto* p = static_cast<const char*>(data);
        size_t sent = 0;
        while (sent < n) {
            const int r = ::send(sock_, p + sent, static_cast<int>(n - sent), 0);
            if (r <= 0) return false;
            sent += static_cast<size_t>(r);
        }
        return true;
    }

    SOCKET      sock_ = INVALID_SOCKET;
    std::string rx_;
};

// ============================================================================
//  事件处理
// ============================================================================
static void handleEvent(WebSocket& ws,
                        long long groupId,
                        double& lastReplySec,
                        const std::string& text) {
    const auto root = json::parse(text);
    if (!root || root->type != json::Value::Type::Object) return;

    // post_type == "message"
    const json::Value* postType = root->find("post_type");
    if (!postType || !postType->isString() || postType->text != "message") return;

    // message_type == "group"
    const json::Value* msgType = root->find("message_type");
    if (!msgType || !msgType->isString() || msgType->text != "group") return;

    // group_id == 监听的群
    const json::Value* gid = root->find("group_id");
    if (!gid || !gid->isInteger() || gid->integer != groupId) return;

    // raw_message
    const json::Value* raw = root->find("raw_message");
    if (!raw || !raw->isString() || raw->text.size() <= 3) return;

    // 关键词匹配
    if (raw->text.find(cfg::kTrigger) == std::string::npos) return;

    // 发言人
    std::string who = "?";
    if (const json::Value* uid = root->find("user_id")) {
        if (uid->isString()) {
            who = uid->text;
        } else if (uid->isInteger()) {
            who = std::to_string(uid->integer);
        }
    }
    if (who.size() > 63) who.resize(63);  // 原版缓冲 64 字节

    const double nowSec = static_cast<double>(::GetTickCount64()) / 1000.0;
    const double remaining = cfg::kCooldownSeconds - (nowSec - lastReplySec);

    if (remaining > 0.0) {
        logmsg("群成员 %s 消息含 \"扣1\"，冷却中（剩余 %d 秒），忽略",
               who.c_str(), static_cast<int>(remaining));
        return;
    }

    char body[256];
    std::snprintf(body, sizeof(body),
                  "{\"action\":\"send_group_msg\",\"params\":{\"group_id\":%lld,\"message\":\"%s\"}}",
                  groupId, cfg::kReply);

    if (!ws.sendFrame(0x1, body, std::strlen(body))) {
        logmsg("发送回复失败，连接可能已断开");
        return;
    }

    lastReplySec = nowSec;
    logmsg("群成员 %s 消息含 \"扣1\"，已回复 \"1\"，进入 1 分钟冷却", who.c_str());
}

// ============================================================================
//  控制台中断处理
// ============================================================================
static BOOL WINAPI onCtrl(DWORD ctrlType) {
    if (ctrlType != CTRL_C_EVENT && ctrlType != CTRL_BREAK_EVENT) return FALSE;

    std::puts("\n已退出。");
    std::fflush(stdout);
    std::exit(0);
}

// ============================================================================
//  main
// ============================================================================
int main() {
    ::SetConsoleOutputCP(65001);  // UTF-8
    ::SetConsoleCP(65001);

    WSADATA wsa{};
    if (::WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        std::puts("WSAStartup 失败，无法初始化网络。");
        return 1;
    }

    ::SetConsoleCtrlHandler(onCtrl, TRUE);

    rng();  // 预热随机数发生器

    // ---------------------------------------------------------------- 界面
    const char* kLine = "==================================================";

    std::puts(kLine);
    std::puts("  Snowluma 群消息自动回复工具（C++ 版）");
    std::puts(kLine);
    std::puts("  功能说明：");
    std::puts("   - 自动连接本机 Snowluma (正向 WebSocket)");
    std::puts("   - 监听指定群聊消息");
    std::puts("   - 消息中包含 \"扣1\" 时立即回复 \"1\"");
    std::puts("   - 触发后全群冷却 1 分钟");
    std::puts("   - 断线自动重连");
    std::printf("  连接地址：ws://%s:%d\n", cfg::kHost, cfg::kPort);
    std::puts(kLine);

    // ------------------------------------------------------------ 输入群号
    long long groupId = 0;
    for (;;) {
        std::printf("\n请输入要监听的群号: ");
        std::fflush(stdout);

        char buf[128] = {0};
        if (std::fgets(buf, static_cast<int>(sizeof(buf)), stdin) == nullptr) {
            groupId = 0;
            break;
        }

        std::string line(buf);
        const size_t first = line.find_first_not_of(" \t");
        if (first == std::string::npos) {
            std::puts("群号必须是纯数字，请重新输入。");
            continue;
        }
        const size_t last = line.find_last_not_of(" \t\r\n");
        line = line.substr(first, last - first + 1);

        if (line.size() > 62) {
            std::puts("群号必须是纯数字，请重新输入。");
            continue;
        }

        const bool allDigits = std::all_of(
            line.begin(), line.end(),
            [](unsigned char c) { return std::isdigit(c) != 0; });
        if (!allDigits) {
            std::puts("群号必须是纯数字，请重新输入。");
            continue;
        }

        groupId = std::strtoll(line.c_str(), nullptr, 10);
        break;
    }

    std::printf("\n已设定监听群号：%lld\n", groupId);
    std::puts("程序运行中，按 Ctrl+C 退出。\n");

    // ------------------------------------------------------ 连接 / 收消息
    WebSocket   ws;
    std::string err;
    double      lastReplySec = 0.0;  // 上次回复的时间点（秒），0 表示从未回复

    for (;;) {
        logmsg("正在连接 ws://%s:%d ...", cfg::kHost, cfg::kPort);

        if (!ws.connectTo(cfg::kHost, cfg::kPort, err)) {
            logmsg("%s，%d 秒后重连...", err.c_str(), cfg::kReconnectDelayMs / 1000);
            std::this_thread::sleep_for(std::chrono::milliseconds(cfg::kReconnectDelayMs));
            continue;
        }

        logmsg("已连接 Snowluma，开始监听群 %lld", groupId);

        std::string accumulator;  // 分片消息累积

        for (;;) {
            int         opcode = 0;
            bool        fin = false;
            std::string payload;

            if (!ws.readFrame(opcode, fin, payload)) {
                logmsg("连接被断开，%d 秒后重连...", cfg::kReconnectDelayMs / 1000);
                break;
            }

            if (opcode == 0x8) {  // close
                logmsg("连接被断开，%d 秒后重连...", cfg::kReconnectDelayMs / 1000);
                break;
            }
            if (opcode == 0x9) {  // ping -> pong
                ws.sendFrame(0xA, payload.data(), payload.size());
                continue;
            }
            if (opcode == 0xA) continue;  // pong

            // 只处理 0（continuation）与 1（text），其余控制/二进制帧忽略
            if ((opcode & 0x0E) != 0) continue;

            accumulator += payload;
            if (!fin) continue;

            std::string message = std::move(accumulator);
            accumulator.clear();

            handleEvent(ws, groupId, lastReplySec, message);
        }

        ws.close();
        std::this_thread::sleep_for(std::chrono::milliseconds(cfg::kReconnectDelayMs));
    }

    ::WSACleanup();
    return 0;
}

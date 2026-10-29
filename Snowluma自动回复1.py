# -*- coding: utf-8 -*-
"""
Snowluma (OneBot v11) 群消息自动回复程序（零依赖版）
功能：连接 Snowluma 正向 WebSocket，监听指定群聊，
      当群内有人发送的消息恰好是 "1" 时，立即回复 "1"，
      之后该群进入 1 小时冷却，冷却期间不再触发。
依赖：无（仅使用 Python 标准库，手写 WebSocket 协议）
"""

import socket
import base64
import hashlib
import struct
import json
import time
import os
import sys

# 控制台使用 UTF-8（配合 bat 中的 chcp 65001，避免中文乱码）
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stdin.reconfigure(encoding="utf-8")
except Exception:
    pass

WS_HOST = "127.0.0.1"
WS_PORT = 3001                   # Snowluma 正向 WebSocket 默认端口
COOLDOWN = 3600                  # 冷却时间：1 小时（秒）
RETRY_DELAY = 3                  # 断线重连间隔（秒）


def banner():
    print("=" * 50)
    print("  Snowluma 群消息自动回复工具（零依赖版）")
    print("=" * 50)
    print("  功能说明：")
    print("   - 自动连接本机 Snowluma (正向 WebSocket)")
    print("   - 监听指定群聊消息")
    print("   - 有人发送消息 \"1\" 时立即回复 \"1\"")
    print("   - 触发后全群冷却 1 小时")
    print("   - 断线自动重连")
    print(f"  连接地址：ws://{WS_HOST}:{WS_PORT}")
    print("=" * 50)


def log(text):
    print(f"[{time.strftime('%H:%M:%S')}] {text}", flush=True)


# ---------------- WebSocket 客户端（标准库实现） ----------------

class WSClient:
    def __init__(self, host, port, path="/"):
        self.sock = None
        self.buf = b""
        self._connect(host, port, path)

    def _connect(self, host, port, path):
        s = socket.create_connection((host, port), timeout=5)
        key = base64.b64encode(os.urandom(16)).decode()
        req = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {host}:{port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "\r\n"
        )
        s.sendall(req.encode())
        resp = b""
        while b"\r\n\r\n" not in resp:
            chunk = s.recv(4096)
            if not chunk:
                s.close()
                raise ConnectionError("握手失败：连接被关闭")
            resp += chunk
        head, self.buf = resp.split(b"\r\n\r\n", 1)
        status = head.split(b"\r\n", 1)[0]
        if b" 101" not in status:
            s.close()
            raise ConnectionError("握手被拒绝: " + status.decode(errors="replace"))
        s.settimeout(None)
        self.sock = s

    def _read_exact(self, n):
        while len(self.buf) < n:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("连接被断开")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n:]
        return data

    def send_text(self, text):
        payload = text.encode("utf-8")
        n = len(payload)
        header = bytearray([0x81])  # FIN + text frame
        if n < 126:
            header.append(0x80 | n)
        elif n < 65536:
            header.append(0x80 | 126)
            header += struct.pack(">H", n)
        else:
            header.append(0x80 | 127)
            header += struct.pack(">Q", n)
        mask = os.urandom(4)
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(bytes(header) + mask + masked)

    def _send_pong(self, payload):
        n = len(payload)
        mask = os.urandom(4)
        header = bytes([0x8A, 0x80 | n])
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        self.sock.sendall(header + mask + masked)

    def recv_text(self):
        """接收一条文本消息；自动回应 ping；连接关闭时抛异常。"""
        while True:
            b1, b2 = self._read_exact(2)
            opcode = b1 & 0x0F
            masked = b2 & 0x80
            length = b2 & 0x7F
            if length == 126:
                length = struct.unpack(">H", self._read_exact(2))[0]
            elif length == 127:
                length = struct.unpack(">Q", self._read_exact(8))[0]
            mask = self._read_exact(4) if masked else None
            payload = self._read_exact(length)
            if mask:
                payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))

            if opcode == 0x8:  # close
                raise ConnectionError("对方关闭了连接")
            if opcode == 0x9:  # ping -> pong
                self._send_pong(payload)
                continue
            if opcode == 0xA:  # pong
                continue
            if opcode in (0x1, 0x0):  # text / continuation
                return payload.decode("utf-8", errors="replace")
            # 其他类型忽略

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass


# ---------------- 主逻辑 ----------------

def run(group_id):
    last_reply = 0.0  # 上次触发回复的时间戳

    while True:
        ws = None
        try:
            log(f"正在连接 ws://{WS_HOST}:{WS_PORT} ...")
            ws = WSClient(WS_HOST, WS_PORT)
            log(f"已连接 Snowluma，开始监听群 {group_id}")

            while True:
                raw = ws.recv_text()
                try:
                    data = json.loads(raw)
                except (json.JSONDecodeError, TypeError):
                    continue

                # 只处理群消息事件
                if data.get("post_type") != "message":
                    continue
                if data.get("message_type") != "group":
                    continue
                if data.get("group_id") != group_id:
                    continue

                msg = (data.get("raw_message") or "").strip()
                if msg != "1":
                    continue

                user_id = data.get("user_id", "?")
                now = time.time()
                remain = COOLDOWN - (now - last_reply)

                if remain > 0:
                    log(f"群成员 {user_id} 发送了 \"1\"，冷却中（剩余 {int(remain // 60)} 分钟），忽略")
                    continue

                # 立即回复，保证最快响应
                ws.send_text(json.dumps({
                    "action": "send_group_msg",
                    "params": {"group_id": group_id, "message": "1"}
                }))
                last_reply = now
                log(f"群成员 {user_id} 发送了 \"1\"，已回复 \"1\"，进入 1 小时冷却")

        except KeyboardInterrupt:
            raise
        except Exception as e:
            log(f"{e}，{RETRY_DELAY} 秒后重连...")
        finally:
            if ws:
                ws.close()

        time.sleep(RETRY_DELAY)


def main():
    banner()
    while True:
        s = input("\n请输入要监听的群号: ").strip()
        if s.isdigit():
            group_id = int(s)
            break
        print("群号必须是纯数字，请重新输入。")

    print(f"\n已设定监听群号：{group_id}")
    print("程序运行中，按 Ctrl+C 退出。\n")

    try:
        run(group_id)
    except KeyboardInterrupt:
        print("\n已退出。")


if __name__ == "__main__":
    main()

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// dummyPowerShellScript 是替代真实 powershell.exe 的夹具：复现 repeater.ps1 的 stdio 协议
// （写就绪字节 \xff、跳过 daemon 写入的脚本源码、读管道名帧，之后逐条转发 agent 消息）。
//
// DUMMY_PS_MODE=agent（默认）按消息体决定行为：
//
//	fail       退出进程（上游崩溃）      linkfail  回链路级失败哨兵（连不上 agent）
//	stuck      永不回复（上游挂起）      slow:<ms> 延迟后回复
//	count      回复本进程已处理的请求数   其他      大写回显
//
// DUMMY_PS_MODE=echo 原样回显 stdin；silent 读满 stdin 后退出（握手失败）。
// 另有 DUMMY_PS_SRC_LEN（脚本字节数）、DUMMY_PS_PIDFILE、DUMMY_PS_LOGFILE（收到的消息）。
const dummyPowerShellScript = `#!/usr/bin/env python3
import os
import struct
import sys
import time

MODE = os.environ.get("DUMMY_PS_MODE") or "agent"
SRC_LEN = int(os.environ.get("DUMMY_PS_SRC_LEN") or "0")
PIDFILE = os.environ.get("DUMMY_PS_PIDFILE") or ""
LOGFILE = os.environ.get("DUMMY_PS_LOGFILE") or ""

inp = sys.stdin.buffer
out = sys.stdout.buffer


def read_exact(n):
    buf = b""
    while len(buf) < n:
        chunk = inp.read(n - len(buf))
        if not chunk:
            return buf
        buf += chunk
    return buf


def send(payload):
    out.write(struct.pack(">I", len(payload)))
    out.write(payload)
    out.flush()


def record(line):
    if LOGFILE:
        with open(LOGFILE, "a") as f:
            f.write(line + "\n")


if PIDFILE:
    with open(PIDFILE, "w") as f:
        f.write(str(os.getpid()))

if MODE == "silent":
    inp.read()
    sys.exit(0)

if MODE == "echo":
    # 先吐噪声再吐就绪字节：验证 daemon 会跳过 0xff 之前的所有输出
    out.write(b"Warning: dummy random warming messages ... Dummy ...\n")
    out.write(b"\xff")
    out.flush()
    while True:
        # 必须用 read1：BufferedReader.read(n) 会攒满 n 字节才返回，
        # 而 daemon 写来的数据长度不确定（脚本源码 + 管道名帧 + 请求）
        chunk = inp.read1(4096)
        if not chunk:
            break
        out.write(chunk)
        out.flush()
    sys.exit(0)

out.write(b"\xff")
out.flush()

# 跳过 daemon 写入的脚本源码，再读管道名帧（uint32 长度 + 名字）
read_exact(SRC_LEN)
header = read_exact(4)
if len(header) < 4:
    sys.exit(1)
read_exact(struct.unpack(">I", header)[0])

served = 0
while True:
    header = read_exact(4)
    if len(header) < 4:
        break
    size = struct.unpack(">I", header)[0]
    body = read_exact(size)
    if len(body) < size:
        break
    served += 1
    record("recv %s" % body.decode("utf-8", "replace"))
    if body == b"fail":
        sys.exit(0)
    if body == b"linkfail":
        # 模拟 repeater.ps1 的链路级失败哨兵（长度 1、类型 0xFF）
        send(b"\xff")
        continue
    if body == b"stuck":
        while True:
            time.sleep(3600)
    if body == b"count":
        send(str(served).encode())
    elif body.startswith(b"slow:"):
        time.sleep(int(body[5:]) / 1000.0)
        send(body.upper())
    else:
        send(body.upper())
`

// requirePython 确认 python3 可用：假 powershell.exe 由 python3 实现。
func requirePython(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Fatalf("需要 python3 才能运行假 powershell.exe: %v", err)
	}
}

// writeDummyPowerShell 把假 powershell.exe 写进 dir 并赋予可执行位。
func writeDummyPowerShell(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, "powershell.exe")
	if err := os.WriteFile(path, []byte(dummyPowerShellScript), 0777); err != nil {
		t.Fatal(err)
	}
}

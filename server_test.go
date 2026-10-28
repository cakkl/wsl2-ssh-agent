package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func setupDummyServer(t *testing.T) string {
	t.Helper()

	tmpDir := setupDummyEnv(t)
	writeDummyPowerShell(t, tmpDir)
	return startDummyServer(t, tmpDir, powershellPath())
}

// setupDummyServerWithoutUpstream 启动守护进程，但上游路径不存在：
// 用于验证「上游起不来时只有该请求失败，守护进程不退出」（I3/I4）。
func setupDummyServerWithoutUpstream(t *testing.T) string {
	t.Helper()

	tmpDir := setupDummyEnv(t)
	// 必须显式指向不存在的路径：不能留空，否则 powershellPath() 会回落到
	// Windows 侧真实的 powershell.exe（WSL 互操作环境下确实存在）
	return startDummyServer(t, tmpDir, filepath.Join(tmpDir, "missing-powershell.exe"))
}

// startDummyServer 在 tmpDir 上起一个守护进程，返回 socket 路径。
func startDummyServer(t *testing.T, tmpDir string, powershell string) string {
	t.Helper()

	path, _ := startDummyServerWithCancel(t, tmpDir, powershell, 0)
	return path
}

// startDummyServerWithCancel 同上，但可以指定 -request-timeout，
// 并额外返回一个 stop 函数（可重复调用）。
func startDummyServerWithCancel(t *testing.T, tmpDir string, powershell string, requestTimeout time.Duration) (string, func()) {
	t.Helper()

	t.Setenv("DUMMY_PS_PIDFILE", filepath.Join(tmpDir, "pid"))
	t.Setenv("DUMMY_PS_LOGFILE", filepath.Join(tmpDir, "recv.log"))

	path := filepath.Join(tmpDir, "tmp.sock")
	s, err := newServer(path, powershell, "dummy-pipe-name", requestTimeout)
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", path, err)
	}

	done := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		s.run(ctx)
		close(done)
	}()

	// cancel 可重复调用，done 关闭后 <-done 也会立即返回
	stop := func() {
		cancel()
		<-done
	}
	t.Cleanup(stop)

	return path, stop
}

// TestServerCancelDuringHandshake：建管道（握手）期间客户端断开时，
// 请求绝不能写入上游（"未写入时取消 = 零副作用"），且守护进程继续服务。
func TestServerCancelDuringHandshake(t *testing.T) {
	// 握手永远等不到就绪字节：把每轮等待拉长，只能靠 ctx 退出
	waitTimesBackup := waitTimes
	t.Cleanup(func() { waitTimes = waitTimesBackup })

	tmpDir := setupDummyEnv(t)
	writeDummyPowerShell(t, tmpDir)
	t.Setenv("DUMMY_PS_MODE", "silent") // 不吐就绪字节，且进程保持存活
	waitTimes = []time.Duration{time.Hour, time.Hour, time.Hour}

	path := startDummyServer(t, tmpDir, powershellPath())

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	start := time.Now()
	if _, err := sock.Write([]byte("\x00\x00\x00\x05hello")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // 让调度层进入握手阶段
	if err := sock.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// 请求必须在握手被 ctx 打断后立刻结束，而不是等满 1 小时
	waitForNoProcess(t, path, 2*time.Second)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("cancellation should interrupt the handshake, took %v", elapsed)
	}

	// 上游一条消息都没收到
	if got := dummyReceived(t, path); len(got) != 0 {
		t.Errorf("cancelled request must never reach upstream, got %v", got)
	}

	// 守护进程存活：还能接受新连接（虽然上游仍不可用，但会立刻失败而不是卡住）
	sock2, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("daemon should stay alive: %v", err)
	}
	defer sock2.Close()
}

// waitForNoProcess 等到假 PowerShell 进程全部消失。
func waitForNoProcess(t *testing.T, socketPath string, limit time.Duration) {
	t.Helper()

	pidFile := filepath.Join(filepath.Dir(socketPath), "pid")
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		buf, err := os.ReadFile(pidFile)
		if err != nil {
			return
		}
		var pid int
		if _, err := fmt.Sscanf(string(buf), "%d", &pid); err != nil {
			return
		}
		if !processAlive(pid) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("upstream process should be gone within %v", limit)
}

// dummyReceived 返回假 PowerShell 收到过的消息（DUMMY_PS_LOGFILE 内容），
// 用于断言「上游恰好被投递一次」（I1）。
func dummyReceived(t *testing.T, socketPath string) []string {
	t.Helper()

	buf, err := os.ReadFile(filepath.Join(filepath.Dir(socketPath), "recv.log"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		t.Fatal(err)
	}

	var lines []string
	for _, line := range strings.Split(string(buf), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestServerNormal(t *testing.T) {
	path := setupDummyServer(t)

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer sock.Close()

	_, err = sock.Write([]byte("\x00\x00\x00\x05hello"))
	if err != nil {
		t.Errorf("failed to communicate: %v", err)
	}

	buf := make([]byte, 4+5)
	n, err := io.ReadFull(sock, buf)
	if err != nil || n != 4+5 || string(buf) != "\x00\x00\x00\x05HELLO" {
		t.Errorf("failed to communicate: %v", err)
	}
}

func TestServerMultipleAccess(t *testing.T) {
	path := setupDummyServer(t)

	sock1, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer sock1.Close()

	sock2, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer sock2.Close()

	_, err = sock1.Write([]byte("\x00\x00\x00\x05sock1"))
	if err != nil {
		t.Errorf("failed to communicate: %v", err)
	}

	_, err = sock2.Write([]byte("\x00\x00\x00\x05sock2"))
	if err != nil {
		t.Errorf("failed to communicate: %v", err)
	}

	buf := make([]byte, 4+5)
	n, err := io.ReadFull(sock1, buf)
	if err != nil || n != 4+5 || string(buf) != "\x00\x00\x00\x05SOCK1" {
		t.Errorf("failed to communicate: %v", err)
	}

	n, err = io.ReadFull(sock2, buf)
	if err != nil || n != 4+5 || string(buf) != "\x00\x00\x00\x05SOCK2" {
		t.Errorf("failed to communicate: %v", err)
	}
}

// TestServerRestart 验证上游空闲时崩溃：守护进程存活，
// 下一条请求会在写入前重建管道并正常完成（I1/I3/I4）。
func TestServerRestart(t *testing.T) {
	path := setupDummyServer(t)

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer sock.Close()

	if _, err := sock.Write([]byte("\x00\x00\x00\x05Hello")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	buf := make([]byte, 4+5)
	if n, err := io.ReadFull(sock, buf); err != nil || n != 4+5 || string(buf) != "\x00\x00\x00\x05HELLO" {
		t.Fatalf("failed to communicate: %v", err)
	}

	// 杀掉假 PowerShell.exe，模拟上游崩溃（空闲状态下死亡）
	proc, err := os.FindProcess(dummyPID(t, path))
	if err != nil {
		t.Fatal("no process found")
	}
	if err := proc.Signal(os.Interrupt); err != nil {
		t.Fatal("failed to kill the process")
	}
	time.Sleep(500 * time.Millisecond) // 留给读循环发现管道已断

	// 下一条请求必须在写入前换成新管道，而不是撞到死进程上
	if _, err := sock.Write([]byte("\x00\x00\x00\x05Hello")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	if n, err := io.ReadFull(sock, buf); err != nil || n != 4+5 || string(buf) != "\x00\x00\x00\x05HELLO" {
		t.Errorf("request should succeed on a rebuilt pipeline: n=%d buf=%q err=%v", n, buf, err)
	}

	// 上游每个管道各收到一条：没有重放（I1）
	if got := dummyReceived(t, path); len(got) != 2 {
		t.Errorf("upstream should receive 1 message per pipeline, got %d: %v", len(got), got)
	}
}

func TestServerFail(t *testing.T) {
	path := setupDummyServer(t)

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer sock.Close()

	_, err = sock.Write([]byte("\x00\x00\x00\x04fail"))
	if err != nil {
		t.Errorf("failed to communicate: %v", err)
	}

	buf := make([]byte, 1)
	_, err = io.ReadFull(sock, buf)
	if err != io.EOF {
		t.Errorf("it should fail with EOF: %v", err)
	}
}

// TestServerCancelKillsPipeline：上游挂起时客户端断开，管道必须立刻被终止（I2/T3），
// 且该请求从未被重放（I1），守护进程继续服务（I3）。
func TestServerCancelKillsPipeline(t *testing.T) {
	path := setupDummyServer(t)

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if _, err := sock.Write([]byte("\x00\x00\x00\x05stuck")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	waitForReceived(t, path, 1) // 确认请求确实已写入上游
	pid := dummyPID(t, path)

	// 模拟 Ctrl+C / 进程被杀：直接关掉客户端连接
	if err := sock.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// 关掉命名管道的等价物：上游进程必须在 1s 内消失
	waitForProcessGone(t, pid, time.Second)

	if got := dummyReceived(t, path); len(got) != 1 {
		t.Errorf("upstream should receive exactly 1 message, got %d: %v", len(got), got)
	}

	// 守护进程存活，新请求照常服务
	sock2, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("daemon should stay alive: %v", err)
	}
	defer sock2.Close()

	if _, err := sock2.Write([]byte("\x00\x00\x00\x05Hello")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	buf := make([]byte, 4+5)
	if n, err := io.ReadFull(sock2, buf); err != nil || n != 4+5 || string(buf) != "\x00\x00\x00\x05HELLO" {
		t.Errorf("daemon should still serve requests: n=%d buf=%q err=%v", n, buf, err)
	}
}

// TestServerQueuedRequestCancelled：请求还在排队时客户端就断开，该请求从不写入上游（I1/T7）；
// 同时验证一条卡住的请求只影响它自己，后来的请求能排队并在管道重建后完成（I3/T4）。
func TestServerQueuedRequestCancelled(t *testing.T) {
	path := setupDummyServer(t)

	// A：占住唯一的管道（上游挂起，永不应答）
	clientA, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	if _, err := clientA.Write([]byte("\x00\x00\x00\x05stuck")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	waitForReceived(t, path, 1)
	pid := dummyPID(t, path)

	// B：只能排队；随后放弃
	clientB, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	if _, err := clientB.Write([]byte("\x00\x00\x00\x06queued")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // 确保 B 的请求已入队
	if err := clientB.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}

	// A 也放弃：调度层会杀管道，随后走到 B 的请求（B 已取消 ⇒ 直接丢弃）
	if err := clientA.Close(); err != nil {
		t.Fatalf("failed to close: %v", err)
	}
	waitForProcessGone(t, pid, time.Second)

	// C 的成功回包证明调度层已经越过了 B 的请求
	clientC, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("daemon should stay alive: %v", err)
	}
	defer clientC.Close()

	if _, err := clientC.Write([]byte("\x00\x00\x00\x05Hello")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	buf := make([]byte, 4+5)
	if n, err := io.ReadFull(clientC, buf); err != nil || n != 4+5 || string(buf) != "\x00\x00\x00\x05HELLO" {
		t.Fatalf("daemon should still serve requests: n=%d buf=%q err=%v", n, buf, err)
	}

	// 上游只见过 stuck 和 Hello；B 的 queued 从未被投递
	got := dummyReceived(t, path)
	if len(got) != 2 {
		t.Errorf("queued request must never reach upstream, got %d: %v", len(got), got)
	}
	for _, line := range got {
		if strings.Contains(line, "queued") {
			t.Errorf("queued request should not be delivered: %v", got)
		}
	}
}

// TestServerShutdownWithIdleClient：有客户端连着但不发数据时，关停仍必须有界
// （原 §3.2 的永久挂死场景）。
func TestServerShutdownWithIdleClient(t *testing.T) {
	tmpDir := setupDummyEnv(t)
	writeDummyPowerShell(t, tmpDir)
	path, stop := startDummyServerWithCancel(t, tmpDir, powershellPath(), 0)

	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	stopped := make(chan struct{})
	go func() {
		stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown should be bounded even with an idle client")
	}

	// 服务端应主动关掉闲置连接
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Errorf("server should close idle connections on shutdown")
	}
}

// dummyPID 读出假 PowerShell 进程的 pid。
func dummyPID(t *testing.T, socketPath string) int {
	t.Helper()

	buf, err := os.ReadFile(filepath.Join(filepath.Dir(socketPath), "pid"))
	if err != nil {
		t.Fatalf("no pid file: %v", err)
	}
	var pid int
	if _, err := fmt.Sscanf(string(buf), "%d", &pid); err != nil {
		t.Fatalf("pid file is wrong: %q", buf)
	}
	return pid
}

func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func waitForProcessGone(t *testing.T, pid int, limit time.Duration) {
	t.Helper()

	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("upstream process %d should be gone within %v", pid, limit)
}

func waitForReceived(t *testing.T, socketPath string, n int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(dummyReceived(t, socketPath)) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("upstream should receive %d message(s), got %v", n, dummyReceived(t, socketPath))
}

// TestServerUpstreamUnavailable 验证上游根本起不来时，只有该请求失败，
// 守护进程不退出（I3/I4，对应原 D7）。
func TestServerUpstreamUnavailable(t *testing.T) {
	path := setupDummyServerWithoutUpstream(t)

	for i := 0; i < 2; i++ {
		sock, err := net.Dial("unix", path)
		if err != nil {
			t.Fatalf("daemon should stay alive (round %d): %v", i, err)
		}

		if _, err := sock.Write([]byte("\x00\x00\x00\x05hello")); err != nil {
			sock.Close()
			t.Fatalf("failed to communicate: %v", err)
		}

		buf := make([]byte, 1)
		if n, err := io.ReadFull(sock, buf); err != io.EOF {
			t.Errorf("request should fail with EOF (round %d): n=%d err=%v", i, n, err)
		}
		sock.Close()
	}
}

// TestServerWriteTimeout：客户端连上却不读响应时，写回必须在 writeTimeout 后放弃，
// 不能把该连接与关停一起拖住（TODO #11）。
func TestServerWriteTimeout(t *testing.T) {
	// writeTimeout 是包级变量：必须在启动 server 之前设置，
	// 并把还原注册得比 server 的 cleanup 更早（LIFO ⇒ 最后执行）
	timeoutBackup := writeTimeout
	t.Cleanup(func() { writeTimeout = timeoutBackup })

	tmpDir := setupDummyEnv(t)
	writeDummyPowerShell(t, tmpDir)
	writeTimeout = 200 * time.Millisecond
	path := startDummyServer(t, tmpDir, powershellPath())

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer sock.Close()

	// 预热：先让上游管道建好，后面的大响应才会立刻开始写（否则要等 PowerShell 启动）
	if got := serverRoundTrip(t, sock, "hello"); got != "HELLO" {
		t.Fatalf("warm-up failed, got %q", got)
	}

	// 1 MiB 的请求（上限）：假上游大写回显同等大小，一定塞满两级 socket 缓冲区
	payload := bytes.Repeat([]byte("a"), maxMessageSize)
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	copy(frame[4:], payload)
	if _, err := sock.Write(frame); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}

	// 关键：这段时间【完全不读】，让服务端的写卡在缓冲区上并触发超时。
	// 等待量 = writeTimeout 的 5 倍，给慢机器留余量。
	time.Sleep(5 * writeTimeout)

	// 服务端此时应已关闭连接：读到的是缓冲区里那部分 + EOF，字节数必然少于完整响应
	if err := sock.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	total := 0
	buf := make([]byte, 64*1024)
	for {
		n, err := sock.Read(buf)
		total += n
		if err == nil {
			continue
		}
		if errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("connection should be closed after write timeout, got %d bytes and no EOF", total)
		}
		break
	}
	if total >= 4+maxMessageSize {
		t.Errorf("write should have been aborted, but the whole %d-byte response arrived", total)
	}

	// 守护进程存活，新连接照常服务（说明超时只影响该连接）
	sock2, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("daemon should stay alive: %v", err)
	}
	defer sock2.Close()
	if got := serverRoundTrip(t, sock2, "hello"); got != "HELLO" {
		t.Errorf("daemon should still serve requests, got %q", got)
	}
}

// TestServerLinkFailure：连不上 Windows agent 时，客户端连接必须被直接关闭、
// **一个字节都不回**（这样 ssh 报「连不上 agent」，而不是「agent 拒绝了请求」）；
// 同时脚本本身仍然可用，下一条请求照常走同一条管道。
func TestServerLinkFailure(t *testing.T) {
	path := setupDummyServer(t)

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if _, err := sock.Write([]byte("\x00\x00\x00\x08linkfail")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}

	// 哨兵 0xFF 绝不能转发给客户端：期望 EOF，且读不到任何字节
	buf := make([]byte, 16)
	n, err := sock.Read(buf)
	if n != 0 || err != io.EOF {
		t.Errorf("link failure must close the connection without a reply: n=%d err=%v", n, err)
	}
	sock.Close()

	// 管道没有被回收：上游仍是同一个进程（PowerShell 只启动过 1 次就够了）
	sock2, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("daemon should stay alive: %v", err)
	}
	defer sock2.Close()

	if got := serverRoundTrip(t, sock2, "hello"); got != "HELLO" {
		t.Errorf("daemon should recover right after a link failure, got %q", got)
	}
}

// TestServerRequestTimeout：配置 -request-timeout 后，卡住的请求在时限内失败、
// 上游只收到 1 条（不重放）、守护进程存活（T5）。
func TestServerRequestTimeout(t *testing.T) {
	tmpDir := setupDummyEnv(t)
	writeDummyPowerShell(t, tmpDir)
	path, _ := startDummyServerWithCancel(t, tmpDir, powershellPath(), 300*time.Millisecond)

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}

	if _, err := sock.Write([]byte("\x00\x00\x00\x05stuck")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}

	start := time.Now()
	buf := make([]byte, 1)
	if n, err := io.ReadFull(sock, buf); err != io.EOF {
		t.Errorf("timed-out request should fail with EOF: n=%d err=%v", n, err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("request should time out promptly, took %v", elapsed)
	}
	sock.Close()

	// 超时也不得重放
	if got := dummyReceived(t, path); len(got) != 1 {
		t.Errorf("upstream should receive exactly 1 message, got %d: %v", len(got), got)
	}

	// 守护进程存活，下个请求成功
	sock2, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("daemon should stay alive: %v", err)
	}
	defer sock2.Close()

	if _, err := sock2.Write([]byte("\x00\x00\x00\x05Hello")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	resp := make([]byte, 4+5)
	if n, err := io.ReadFull(sock2, resp); err != nil || n != 4+5 || string(resp) != "\x00\x00\x00\x05HELLO" {
		t.Errorf("daemon should still serve requests: n=%d buf=%q err=%v", n, resp, err)
	}
}

// TestServerRejectsHugeMessage：长度字段超过上限时直接拒绝，
// 不会按客户端要求分配内存（原 D10）。
func TestServerRejectsHugeMessage(t *testing.T) {
	path := setupDummyServer(t)

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer sock.Close()

	// 长度字段 0x7fffffff ≈ 2 GiB
	if _, err := sock.Write([]byte("\x7f\xff\xff\xff")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}

	buf := make([]byte, 1)
	if n, err := io.ReadFull(sock, buf); err != io.EOF {
		t.Errorf("oversized message should close the connection: n=%d err=%v", n, err)
	}

	// 守护进程存活，后续请求正常
	sock2, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("daemon should stay alive: %v", err)
	}
	defer sock2.Close()

	if _, err := sock2.Write([]byte("\x00\x00\x00\x05Hello")); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}
	resp := make([]byte, 4+5)
	if n, err := io.ReadFull(sock2, resp); err != nil || n != 4+5 || string(resp) != "\x00\x00\x00\x05HELLO" {
		t.Errorf("daemon should still serve requests: n=%d buf=%q err=%v", n, resp, err)
	}
}

// TestServerPipelineReuse：正常请求复用同一条热管道，且每条请求在上游只执行一次；
// 上游「慢但会回包」时只是等待，不能重放（I1/T1/T2）。
func TestServerPipelineReuse(t *testing.T) {
	path := setupDummyServer(t)

	sock, err := net.Dial("unix", path)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer sock.Close()

	// count 回复「本上游进程累计处理过的条数」：
	// 若请求被重放，计数会跳得比 i 大；若每次都新建进程，计数会一直回到 1。
	for i := 1; i <= 3; i++ {
		if got := serverRoundTrip(t, sock, "count"); got != strconv.Itoa(i) {
			t.Errorf("upstream should have served %d request(s), got %q", i, got)
		}
	}

	start := time.Now()
	if got := serverRoundTrip(t, sock, "slow:100"); got != "SLOW:100" {
		t.Errorf("slow response should be returned as-is, got %q", got)
	}
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Errorf("response should be delayed by the upstream, took %v", elapsed)
	}
}

// serverRoundTrip 按 agent 帧格式发送一条消息并读回响应体。
func serverRoundTrip(t *testing.T, conn net.Conn, payload string) string {
	t.Helper()

	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	copy(frame[4:], payload)

	if _, err := conn.Write(frame); err != nil {
		t.Fatalf("failed to communicate: %v", err)
	}

	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		t.Fatalf("failed to read response header: %v", err)
	}
	body := make([]byte, binary.BigEndian.Uint32(header))
	if _, err := io.ReadFull(conn, body); err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	return string(body)
}

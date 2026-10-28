package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestTimeoutBudget：几个超时常量之间存在硬性大小关系，改单个值很容易破坏，
// 破坏后的症状是「正常 -stop 却以退出码 1 结束」，systemd 会把进程重新拉起。
func TestTimeoutBudget(t *testing.T) {
	// 关停最坏耗时 = 等客户端把应答写完 + 等读循环退出 + 等 exec 的 I/O 复制结束
	if worst := writeTimeout + killTimeout + waitDelay; shutdownGrace <= worst {
		t.Errorf("shutdownGrace (%v) must exceed writeTimeout+killTimeout+waitDelay (%v), "+
			"otherwise the grace exit fires before shutdown completes", shutdownGrace, worst)
	}
	// -stop 必须比守护进程自己的兜底更晚放弃
	if stopTimeout <= shutdownGrace {
		t.Errorf("stopTimeout (%v) must exceed shutdownGrace (%v)", stopTimeout, shutdownGrace)
	}
}

// TestOutputFormatQuoting：socket 路径要按 shell 规则加引号，含空格/单引号时 eval 也不能坏。
func TestOutputFormatQuoting(t *testing.T) {
	const path = "/tmp/a b.sock"

	cases := []struct{ format, want string }{
		{"sh", `SSH_AUTH_SOCK='/tmp/a b.sock'; export SSH_AUTH_SOCK;`},
		{"zsh", `SSH_AUTH_SOCK='/tmp/a b.sock'; export SSH_AUTH_SOCK;`},
		{"csh", `setenv SSH_AUTH_SOCK '/tmp/a b.sock'`},
		{"fish", `set -x SSH_AUTH_SOCK '/tmp/a b.sock'`},
	}
	for _, c := range cases {
		got := fmt.Sprintf(getOutputFormat(c.format), shellQuote(path))
		if got != c.want {
			t.Errorf("%s: got %q, want %q", c.format, got, c.want)
		}
	}

	// 路径里带单引号：交给真实的 sh 去 eval，验证转义正确
	tricky := filepath.Join(t.TempDir(), "it's a sock")
	out := fmt.Sprintf(getOutputFormat("sh"), shellQuote(tricky))
	cmd := exec.Command("sh", "-c", `eval "$1"; printf '%s' "$SSH_AUTH_SOCK"`, "sh", out)
	got, err := cmd.Output()
	if err != nil {
		t.Fatalf("eval failed: %v (script: %s)", err, out)
	}
	if string(got) != tricky {
		t.Errorf("eval gave %q, want %q (script: %s)", got, tricky, out)
	}
}

// TestPidFile：pid 文件能读写、能被清除，且会核对 /proc 里的进程名。
func TestPidFile(t *testing.T) {
	socketPath := filepath.Join(t.TempDir(), "agent.sock")

	if _, ok := readPidFile(socketPath); ok {
		t.Errorf("readPidFile should report missing file")
	}

	if err := writePidFile(socketPath); err != nil {
		t.Fatalf("writePidFile: %v", err)
	}
	t.Cleanup(func() { removePidFile(socketPath) })

	pid, ok := readPidFile(socketPath)
	if !ok || pid != os.Getpid() {
		t.Errorf("readPidFile = %d,%v; want %d,true", pid, ok, os.Getpid())
	}

	// 测试二进制路径里含 wsl2-ssh-agent，因此应被认作本程序
	if !isOurDaemon(pid) {
		t.Errorf("isOurDaemon(%d) should be true", pid)
	}
	// 不存在的 pid 不能认
	if isOurDaemon(1 << 22) {
		t.Errorf("isOurDaemon should be false for a missing process")
	}

	// 内容损坏时视为没有 pid 文件（-stop 会退回 socket 探测）
	if err := os.WriteFile(pidFilePath(socketPath), []byte("not-a-pid"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, ok := readPidFile(socketPath); ok {
		t.Errorf("readPidFile should reject garbage")
	}

	removePidFile(socketPath)
	if _, err := os.Stat(pidFilePath(socketPath)); !os.IsNotExist(err) {
		t.Errorf("pid file should be removed, stat err = %v", err)
	}
}

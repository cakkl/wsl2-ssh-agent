package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

// daemonizeEnv 由父进程设置，用来告诉子进程「你就是守护进程」。
// 不靠「fd 3 是否打开」判断：fd 3 可能被父进程/终端继承，会误判。
const daemonizeEnv = "WSL2_SSH_AGENT_DAEMONIZING"

// check if this process is a daemonized child or not
func checkDaemonMode() bool {
	if os.Getenv(daemonizeEnv) != "1" {
		return true
	}

	// 子进程必须拿到父进程通过 ExtraFiles 递过来的 fd 3
	if _, err := unix.FcntlInt(3, unix.F_GETFD, 0); err != nil {
		log.Fatalf("daemonized process has no handshake pipe on fd 3: %s", err)
	}
	syscall.CloseOnExec(3)

	return false
}

// invoke itself as a child process
func startDaemonizing(args ...string) {
	r, w, err := os.Pipe()
	if err != nil {
		fatal("failed to create a handshake pipe: %s", err)
	}

	exe, err := os.Executable()
	if err != nil {
		fatal("failed to locate the executable: %s", err)
	}

	// invoke itself
	cmd := exec.Command(exe, args...)
	cmd.ExtraFiles = []*os.File{w}
	cmd.Env = append(os.Environ(), daemonizeEnv+"=1")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Start()
	if err != nil {
		fatal("failed to start the daemon: %s", err)
	}

	// wait for the child process
	w.Close()
	buf, err := io.ReadAll(r)
	if err != nil {
		fatal("failed to read the handshake pipe: %s", err)
	}
	r.Close()

	// 子进程只有在监听成功之后才会写这一行；读不到说明它已经失败退出
	if len(buf) == 0 {
		fatal("the daemon exited without reporting success; see the message above")
	}

	// output the script
	fmt.Println(string(buf))

	os.Stdin.Close()

	os.Exit(0)
}

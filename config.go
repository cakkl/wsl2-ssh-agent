package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type config struct {
	socketPath     string
	powershellPath string
	pipeName       string
	requestTimeout time.Duration
	format         string
	foreground     bool
	verbose        bool
	stop           bool
	logFile        string
	version        bool

	// output 是要让 shell eval 的那行脚本
	output string
	// childPipe 是 daemonize 子进程的握手管道（fd 3）；前台模式为 nil
	childPipe *os.File
}

// shutdownGrace 是「收到信号后仍未退出」的兜底时限。
const shutdownGrace = 5 * time.Second

// stopTimeout 是 -stop 等待进程消失的上限。
// 必须比 shutdownGrace 长：否则 -stop 会撞上守护进程兜底自杀的那一刻判超时，
// 而在 systemd 下 os.Exit(1) 又会触发 Restart=on-failure，把刚停掉的进程拉起来。
const stopTimeout = shutdownGrace + 5*time.Second

var version = "(development version)"
var logOutput io.Writer

// fatal 写 stderr 后退出。不能用 log.Fatal：默认日志目标是 io.Discard，
// 失败会完全静默（用户只看到 eval 之后什么都没发生）。
func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "wsl2-ssh-agent: "+format+"\n", args...)
	os.Exit(1)
}

func defaultSocketPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	return filepath.Join(home, ".ssh", "wsl2-ssh-agent.sock")
}

func powershellPath() string {
	path, err := exec.LookPath("powershell.exe")
	if err != nil {
		path := "/mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe"
		_, err := os.Stat(path)
		if err == nil {
			return path
		} else {
			return ""
		}
	}
	return path
}

func newConfig() *config {
	c := &config{}

	flag.StringVar(&c.socketPath, "socket", defaultSocketPath(), "a path of UNIX domain socket to listen")
	flag.StringVar(&c.powershellPath, "powershell-path", powershellPath(), "a path of Windows PowerShell")
	flag.StringVar(&c.pipeName, "pipename", "openssh-ssh-agent", "a name of pipe to connect")
	flag.DurationVar(&c.requestTimeout, "request-timeout", 0, "a maximum duration to wait for a response from Windows (0 = no timeout)")
	flag.BoolVar(&c.foreground, "foreground", false, "run in foreground mode")
	flag.BoolVar(&c.verbose, "verbose", false, "verbose mode")
	flag.StringVar(&c.logFile, "log", "", "a file path to write the log")
	flag.StringVar(&c.format, "format", "auto", "an output format: auto, bash, zsh, csh, tcsh, or fish")
	flag.BoolVar(&c.stop, "stop", false, "stop the daemon and exit")
	flag.BoolVar(&c.version, "version", false, "print version and exit")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "usage: wsl2-ssh-agent\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if c.powershellPath == "" {
		fmt.Fprintln(os.Stderr, "powershell.exe not found, use the -powershell-path to customize the path.")
		os.Exit(1)
	}

	return c
}

// shellQuote 用单引号包住路径，并把路径里的每个单引号替换成「闭引号+转义+开引号」，
// 这样 socket 路径含空格或引号时 eval 也安全。
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func getOutputFormat(format string) string {
	if format == "auto" {
		shell := os.Getenv("SHELL")
		if strings.HasSuffix(shell, "fish") {
			format = "fish"
		} else if strings.HasSuffix(shell, "csh") {
			format = "csh"
		} else {
			format = "sh"
		}
	}
	switch format {
	case "sh", "bash", "zsh":
		return "SSH_AUTH_SOCK=%s; export SSH_AUTH_SOCK;"
	case "csh", "tcsh":
		return "setenv SSH_AUTH_SOCK %s"
	case "fish":
		return "set -x SSH_AUTH_SOCK %s"
	default:
		fmt.Fprintf(os.Stderr, "output format must be auto, bash, zsh, csh, tcsh, or fish\n")
		os.Exit(1)
		return ""
	}
}

func (c *config) start() context.Context {
	if c.version {
		fmt.Printf("wsl2-ssh-agent %s\n", version)
		os.Exit(0)
	}

	// check if this process is a child or not
	parent := checkDaemonMode()

	// script output
	c.output = fmt.Sprintf(getOutputFormat(c.format), shellQuote(c.socketPath))

	// set up the log file
	c.setupLogFile()

	// check if wsl2-ssh-agent is already running
	serverPid := findDaemonPid(c.socketPath)

	// --stop option
	if c.stop {
		if serverPid == -1 {
			// pid/socket 指向的进程已经不在了：把残留文件清掉，再报未运行
			removePidFile(c.socketPath)
			fatal("failed to find a running wsl2-ssh-agent (socket: %s)", c.socketPath)
		}

		log.Printf("kill wsl2-ssh-agent (pid: %d)", serverPid)
		if err := stopService(serverPid); err != nil {
			fatal("%s", err)
		}
		removePidFile(c.socketPath)
		os.Exit(0)
	}

	// avoid multiple start
	if serverPid != -1 {
		log.Printf("wsl2-ssh-agent (pid: %d) is already running; exit", serverPid)
		fmt.Println(c.output)
		os.Exit(0)
	}

	// daemonize
	if !c.foreground {
		if parent {
			log.Printf("daemonize: start")
			startDaemonizing(os.Args[1:]...)
			// startDaemonizing 不会返回
		}

		// 子进程：先保留握手管道与 stdio，等监听成功后再通知父进程（见 ready）。
		// 这样「打印 export 行」就等价于「守护进程真的在监听」。
		c.childPipe = os.NewFile(3, "handshake")
	}

	// set up signal handlers
	signal.Ignore(syscall.SIGPIPE)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// 第一次信号：恢复正常处置，于是第二个信号会直接杀掉进程；
	// 再加一个时限兜底，避免关停路径出问题时永久卡住（原 D9）
	go func() {
		<-ctx.Done()
		stop()
		<-time.After(shutdownGrace)
		log.Printf("shutdown is taking too long; exit")
		os.Exit(1)
	}()

	return ctx
}

// ready 在监听成功之后调用：通知父进程、脱离终端。
// 前台模式与「已有守护进程」路径不会走到这里，此时什么都不做。
func (c *config) ready() {
	if c.childPipe == nil {
		return
	}

	if _, err := c.childPipe.WriteString(c.output); err != nil {
		fatal("failed to notify the parent process: %s", err)
	}
	c.childPipe.Close()
	c.childPipe = nil

	// stdio 必须在通知之后关：监听失败时要能把它写到终端上
	os.Stdin.Close()
	os.Stdout.Close()
	os.Stderr.Close()

	if _, err := syscall.Setsid(); err != nil {
		log.Printf("failed to setsid: %s", err)
	}
	log.Printf("daemonize: completed")
}

func (c *config) setupLogFile() {
	var logFile *os.File

	if c.logFile != "" {
		f, err := os.OpenFile(c.logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
		if err != nil {
			fatal("failed to open a log file: %s", err)
		}
		logFile = f
	}

	if logFile != nil {
		if c.verbose {
			logOutput = io.MultiWriter(logFile, os.Stderr)
		} else {
			logOutput = logFile
		}
	} else {
		if c.verbose {
			// 用 stderr 而不是 stdout：stdout 会被 eval $(...) 捕获，
			// 日志混进去会被当成 shell 命令执行
			logOutput = os.Stderr
		} else {
			logOutput = io.Discard
		}
	}
	log.SetOutput(logOutput)
	log.SetPrefix("[L] ")
}

// pidFilePath 是守护进程记录自身 pid 的位置（与 socket 同目录）。
func pidFilePath(socketPath string) string {
	return socketPath + ".pid"
}

// writePidFile 让 -stop 能直接找到本进程，不必依赖 socket 探测。
func writePidFile(socketPath string) error {
	return os.WriteFile(pidFilePath(socketPath), []byte(strconv.Itoa(os.Getpid())), 0600)
}

func removePidFile(socketPath string) {
	_ = os.Remove(pidFilePath(socketPath))
}

// readPidFile 读出 pid 文件里的 pid；文件不存在或内容异常时返回 false。
func readPidFile(socketPath string) (int, bool) {
	buf, err := os.ReadFile(pidFilePath(socketPath))
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(buf)))
	if err != nil || pid <= 0 {
		return 0, false
	}
	return pid, true
}

// isOurDaemon 确认 pid 确实是 wsl2-ssh-agent：只看 /proc/<pid>/exe，否则 `vim wsl2-ssh-agent.go`
// 这类进程也会被误判；读不到时返回 false——宁可退回 socket 探测，也不猜。
func isOurDaemon(pid int) bool {
	target, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil {
		return false
	}
	return strings.Contains(filepath.Base(target), "wsl2-ssh-agent")
}

// findDaemonPid 依次尝试：pid 文件 → socket 探测。返回 -1 表示没有在运行。
func findDaemonPid(socketPath string) int {
	if pid, ok := readPidFile(socketPath); ok && isOurDaemon(pid) {
		return pid
	}
	return findRunningServerPid(socketPath)
}

// find the existing server by getsockopt
func findRunningServerPid(path string) int {
	// try to connect to the existing server via UNIX domain socket
	conn, err := net.Dial("unix", path)

	if err != nil {
		// failed to connect
		if errors.Is(err, syscall.ENOENT) {
			// no UNIX domain socket; cannot find the server
			return -1
		}
		if errors.Is(err, syscall.ECONNREFUSED) {
			// presumably the existing server has already aborted;
			// remove the UNIX domain socket
			err = os.Remove(path)
			if err != nil {
				fatal("failed to remove %s: %s", path, err)
			}
			return -1
		}
		fatal("failed to connect to %s: %s", path, err)
	}

	// connected
	defer conn.Close()

	// identify the pid of the existing server
	file, err := conn.(*net.UnixConn).File()
	if err != nil {
		fatal("%s", err)
	}
	defer file.Close()
	cred, err := syscall.GetsockoptUcred(int(file.Fd()), syscall.SOL_SOCKET, syscall.SO_PEERCRED)
	if err != nil {
		fatal("%s", err)
	}

	return int(cred.Pid)
}

// stop the running server
func stopService(pid int) error {
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := process.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	// 该进程不是我们的子进程，所以不能 process.Wait()
	// （会以 waitid: no child processes 失败）；只能轮询它是否消失
	deadline := time.Now().Add(stopTimeout)
	for time.Now().Before(deadline) {
		if !processExists(pid) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("wsl2-ssh-agent (pid: %d) did not exit within %v", pid, stopTimeout)
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

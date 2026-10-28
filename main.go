package main

import "log"

func main() {
	c := newConfig()

	ctx := c.start()

	s, err := newServer(c.socketPath, c.powershellPath, c.pipeName, c.requestTimeout)
	if err != nil {
		// 监听失败：绝不向父进程输出 export 行，让 eval 什么都不做，并在终端上说清原因
		fatal("failed to listen on %s: %s", c.socketPath, err)
	}

	// 记录 pid，-stop 就不必靠 socket 探测（探测拿到的 pid 可能已被复用）。
	// 放在 ready() 之前：它关闭 stdio，之后再出错就看不到日志了。
	if err := writePidFile(c.socketPath); err != nil {
		log.Printf("failed to write the pid file: %s", err)
	}
	defer removePidFile(c.socketPath)

	// 到这里守护进程真的在监听了，可以放心把脚本交给父进程
	c.ready()

	// run 只在监听器意外出错时返回错误：以非零退出码退出，让 systemd 等拉起
	if err := s.run(ctx); err != nil {
		// fatal 走 os.Exit，不会执行上面的 defer，所以这里先清掉 pid 文件
		removePidFile(c.socketPath)
		fatal("aborted: %s", err)
	}
}

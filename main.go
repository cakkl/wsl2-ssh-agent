package main

func main() {
	c := newConfig()

	ctx := c.start()

	s, err := newServer(c.socketPath, c.powershellPath, c.pipeName, c.requestTimeout)
	if err != nil {
		// 监听失败：绝不向父进程输出 export 行，让 eval 什么都不做，并在终端上说清原因
		fatal("failed to listen on %s: %s", c.socketPath, err)
	}

	// 到这里守护进程真的在监听了，可以放心把脚本交给父进程
	c.ready()

	// run 只在监听器意外出错时返回错误：以非零退出码退出，让 systemd 等拉起
	if err := s.run(ctx); err != nil {
		fatal("aborted: %s", err)
	}
}

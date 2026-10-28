package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"
)

// queueSize 是请求队列容量：满了以后客户端会在 queue <- 上等待，且随时可被取消。
const queueSize = 128

// writeTimeout 是「把应答写给客户端」的上限。客户端连上却不读时，
// 写会卡在 socket 缓冲区上，拖住该连接与整体关停。
var writeTimeout = 5 * time.Second

type server struct {
	listener net.Listener
	upstream *upstream
	queue    chan *request

	// requestTimeout 为 0 表示不设超时（与 ssh.exe 一致，由用户用 Ctrl+C 取消）
	requestTimeout time.Duration
}

func newServer(socketPath string, powershellPath string, pipeName string, requestTimeout time.Duration) (*server, error) {
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return nil, err
	}
	log.Printf("start listening on %s", socketPath)

	return &server{
		listener: listener,
		upstream: &upstream{powershellPath: powershellPath, pipeName: pipeName},
		queue:    make(chan *request, queueSize),

		requestTimeout: requestTimeout,
	}, nil
}

// response 是一次 SSH agent 调用的结果：要么是上游的应答，要么是该请求的错误。
type response struct {
	data []byte
	err  error
}

// request 表示一次客户端调用。result 容量为 1，服务端投递时永不阻塞。
// ctx 的生命周期 = 客户端连接 ∪ 守护进程关停：客户端一断开就取消，
// 调度层据此立刻终止承载该请求的上游管道（I2）。
type request struct {
	data   []byte
	ctx    context.Context
	result chan response
}

var (
	// errShuttingDown 表示守护进程正在关停，客户端应视为本次操作失败。
	errShuttingDown = errors.New("wsl2-ssh-agent is shutting down")
	// errPipelineDied 表示上游管道在应答前就断了（进程崩溃/退出）。
	errPipelineDied = errors.New("upstream pipeline died")
	// errAgentUnavailable 表示根本连不上 Windows 侧的 agent（命名管道不存在）。
	// 这类失败不向客户端回任何字节，让 ssh 报「连不上 agent」而不是「agent 拒绝了请求」。
	errAgentUnavailable = errors.New("cannot reach the Windows ssh-agent (named pipe missing)")
)

// linkFailureType 是 repeater.ps1 用来表示「连不上 Windows 侧 agent」的哨兵字节。
// 它不属于 SSH agent 协议（真实 agent 绝不会发），必须在这里拦下、不能转发给客户端。
const linkFailureType = 0xff

func isLinkFailure(msg []byte) bool {
	return len(msg) == 5 && msg[4] == linkFailureType
}

// fail / complete 都是非阻塞投递：客户端可能已经放弃，服务端不能因此卡住。
func (r *request) fail(err error) {
	select {
	case r.result <- response{err: err}:
	default:
	}
}

func (r *request) complete(data []byte) {
	select {
	case r.result <- response{data: data}:
	default:
	}
}

// run 处理连接直到关停。返回非 nil 表示监听器意外出错（非正常关停），
// 调用方应据此以非零退出码退出，让 systemd 等拉起（I5）。
func (s *server) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer s.upstream.closeAll()
	defer cancel()

	go func() {
		<-ctx.Done()
		log.Println("shutdown")
		s.listener.Close()
	}()

	// invoke gorountine for ssh-agent.exe
	done := make(chan struct{})
	go func() {
		s.server(ctx, done)
	}()

	var acceptErr error
	wg := &sync.WaitGroup{}
	for {
		// wait for connection
		sshClient, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				// 不是我们主动关的 listener ⇒ 非正常关停
				log.Printf("failed to accept: %s", err)
				acceptErr = err
			}
			cancel()
			break
		}

		// invoke goroutine for ssh client
		log.Printf("ssh: connected")
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.client(ctx, sshClient)
		}()
	}

	// wait for all ssh clients to disconnect
	wg.Wait()

	// wait for ssh-agent.exe to exit
	close(s.queue)
	<-done

	return acceptErr
}

// server 依次处理请求队列：同一时刻只有一条上游管道、一条 in-flight 请求。
// 请求要么写入上游、要么没写入；写入之后绝不重放（I1），
// 任何单条请求的失败只影响它自己与它所在的连接（I3），不会让守护进程退出（I4）。
func (s *server) server(ctx context.Context, done chan struct{}) {
	defer close(done)

	// 关停时把排队中的请求全部失败掉，并持续消费队列：
	// 否则仍可能有客户端阻塞在 queue <- 上
	defer func() {
		for req := range s.queue {
			req.fail(errShuttingDown)
		}
	}()

	for {
		var req *request
		select {
		case <-ctx.Done():
			return
		case r, ok := <-s.queue:
			if !ok {
				return
			}
			req = r
		}
		s.handle(req)
	}
}

// handle 单飞地执行一条请求。返回时该请求一定有结果，且上游最多被写入一次（I1）。
func (s *server) handle(req *request) {
	// 排队期间客户端就走了：从未写入上游，直接丢弃
	if err := req.ctx.Err(); err != nil {
		log.Printf("request dropped before delivery: %s", err)
		req.fail(err)
		return
	}

	// 用「请求」的 ctx 建管道：客户端断开/超时/关停都能打断握手，
	// 否则客户端走了还得白等最长 19 秒的握手重试（而且随后还会把请求写进上游）
	p, err := s.upstream.get(req.ctx)
	if err != nil {
		// 从未写入 ⇒ 不重放，只让这条请求失败（I3/I4）
		log.Printf("failed to invoke [W]: %s", err)
		req.fail(err)
		return
	}

	// 建管道可能花了好几秒（握手重试）。期间客户端可能已经走了：
	// 此时绝不能写入上游，否则会白弹一次授权框（"未写入时取消 = 零副作用"）。
	if err := req.ctx.Err(); err != nil {
		log.Printf("request dropped before delivery: %s", err)
		s.discardPipeline(p)
		req.fail(err)
		return
	}

	if _, err := p.in.Write(req.data); err != nil {
		// 送达状态未知 ⇒ 不重放（宁可在客户端侧报错）
		log.Printf("failed to write to [W]: %s", err)
		s.discardPipeline(p)
		req.fail(err)
		return
	}
	log.Printf("[L] -> [W] (%d B)", len(req.data))

	select {
	case resp, ok := <-p.responses:
		if !ok {
			log.Printf("upstream pipeline died")
			s.discardPipeline(p)
			req.fail(errPipelineDied)
			return
		}
		if isLinkFailure(resp) {
			// 脚本没能连上 agent。管道本身是健康的（脚本继续跑），不回收进程；
			// 但必须让客户端看到「连不上」，而不是「被拒绝」。
			log.Printf("%s", errAgentUnavailable)
			req.fail(errAgentUnavailable)
			return
		}
		log.Printf("[L] <- [W] (%d B)", len(resp))
		req.complete(resp)
	case <-req.ctx.Done():
		// 取消：关闭命名管道 ⇒ Agent 感知客户端断开，取消待处理的授权弹窗
		log.Printf("request cancelled: %s", req.ctx.Err())
		s.discardPipeline(p)
		req.fail(req.ctx.Err())
	}
}

func (s *server) discardPipeline(p *pipeline) {
	s.upstream.drop(p)
	p.kill()
}

// upstream 管理上游管道。v1 只有一条热管道（串行复用，与后端本身的串行模型一致）；
// 接口按 get/drop/closeAll 抽象，以后加池不需要改 client/handle（见设计文档 §9.1）。
type upstream struct {
	powershellPath string
	pipeName       string

	mu  sync.Mutex
	cur *pipeline
}

func (u *upstream) get(ctx context.Context) (*pipeline, error) {
	// 已经取消/关停就不要再拉起 PowerShell：否则会在关停时白花最长 19 秒（握手重试）
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	u.mu.Lock()
	if u.cur != nil && u.cur.alive() {
		p := u.cur
		u.mu.Unlock()
		return p, nil
	}
	stale := u.cur
	u.cur = nil
	u.mu.Unlock()

	if stale != nil {
		stale.kill() // 回收已死的管道（含 cmd.Wait，避免僵尸进程）
	}

	p, err := newPipeline(ctx, u.powershellPath, u.pipeName)
	if err != nil {
		return nil, err
	}

	u.mu.Lock()
	u.cur = p
	u.mu.Unlock()
	return p, nil
}

// drop 表示这条管道已不可用；调用方负责 kill。
func (u *upstream) drop(p *pipeline) {
	u.mu.Lock()
	if u.cur == p {
		u.cur = nil
	}
	u.mu.Unlock()
}

func (u *upstream) closeAll() {
	u.mu.Lock()
	p := u.cur
	u.cur = nil
	u.mu.Unlock()

	if p != nil {
		p.kill()
	}
}

// pipeline 是一条上游通道：一个 PowerShell 进程 + 它连着的命名管道。
type pipeline struct {
	in  io.WriteCloser
	out io.Reader
	cmd *exec.Cmd

	responses chan []byte   // 容量 1；由 readLoop 负责关闭
	dead      chan struct{} // 读循环结束或被 kill 时关闭（表示不可再用）
	done      chan struct{} // 读循环已退出
	closeOnce sync.Once
}

func newPipeline(ctx context.Context, powershell string, pipename string) (*pipeline, error) {
	rep, err := newRepeater(ctx, powershell, pipename)
	if err != nil {
		return nil, err
	}

	p := &pipeline{
		in:        rep.in,
		out:       rep.out,
		cmd:       rep.cmd,
		responses: make(chan []byte, 1),
		dead:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	go p.readLoop()

	return p, nil
}

// readLoop 独立于请求运行，这样调度层可以同时等待「响应」与「取消」。
func (p *pipeline) readLoop() {
	defer close(p.done)
	defer close(p.responses)

	for {
		resp, err := readMessage(p.out)
		if err != nil {
			p.markDead()
			return
		}
		select {
		case p.responses <- resp:
		case <-p.dead:
			return
		}
	}
}

func (p *pipeline) markDead() {
	p.closeOnce.Do(func() { close(p.dead) })
}

func (p *pipeline) alive() bool {
	select {
	case <-p.dead:
		return false
	default:
		return true
	}
}

// kill 同时是「取消」和「回收」：关闭命名管道让 Agent 感知断开（取消待处理弹窗），
// 再杀掉 PowerShell——脚本阻塞在 Windows 侧管道读上，只能靠进程退出中断。幂等。
func (p *pipeline) kill() {
	p.markDead()

	_ = p.in.Close()
	if p.cmd.Process != nil {
		if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			log.Printf("failed to kill [W]: %s", err)
		}
	}

	// os/exec 要求 StdoutPipe 读完之后才能 Wait
	<-p.done
	_ = p.cmd.Wait()
}

// client 服务一条 SSH 客户端连接：一个读 goroutine + 一个处理循环。
// 客户端断开必须在任何时刻都能被感知（否则请求无法取消），所以读与等待分成两边，
// 两边共享同一个连接级 ctx。
func (s *server) client(ctx context.Context, conn net.Conn) {
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer conn.Close()

	msgs := make(chan []byte, 1)
	go func() {
		defer close(msgs)
		for {
			msg, err := readMessage(conn)
			if err != nil {
				// 连接是我们自己关的（处理循环已返回）就不必再报错
				if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
					log.Printf("failed to read from ssh: %s", err)
				}
				cancel() // 关键：EOF/RST 立刻变成 ctx 取消
				return
			}
			select {
			case msgs <- msg:
			case <-connCtx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-connCtx.Done():
			return
		case msg, ok := <-msgs:
			if !ok {
				log.Printf("ssh: closed")
				return
			}
			log.Printf("ssh -> [L] (%d B)", len(msg))

			resp := s.exchange(connCtx, msg)
			if resp.err != nil {
				// 与 ssh.exe 一致：agent 出错即本次操作失败，不重放、不留半死连接
				log.Printf("request failed: %s", resp.err)
				return
			}
			if err := conn.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
				log.Printf("failed to set write deadline: %s", err)
			}
			_, err := conn.Write(resp.data)
			// 无论成败都清掉期限，否则同一连接的下一次写会带着旧期限
			_ = conn.SetWriteDeadline(time.Time{})
			if err != nil {
				log.Printf("failed to write to ssh: %s", err)
				return
			}
			log.Printf("ssh <- [L] (%d B)", len(resp.data))
		}
	}
}

// exchange 把一条请求送入调度层并等待结果。
// 期间可被两种事件打断：客户端断开、以及可选的 -request-timeout（只让该请求失败）。
func (s *server) exchange(connCtx context.Context, data []byte) response {
	reqCtx, cancel := s.requestContext(connCtx)
	defer cancel()

	req := &request{data: data, ctx: reqCtx, result: make(chan response, 1)}
	select {
	case s.queue <- req:
	case <-reqCtx.Done():
		return response{err: reqCtx.Err()}
	}

	select {
	case resp := <-req.result:
		return resp
	case <-reqCtx.Done():
		return response{err: reqCtx.Err()}
	}
}

// requestContext 给一条请求派生生命周期：客户端连接 ∪ 可选的 -request-timeout。
// 超时只让该请求失败，绝不重放（I6）。
func (s *server) requestContext(parent context.Context) (context.Context, context.CancelFunc) {
	if s.requestTimeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, s.requestTimeout)
}

// maxMessageSize 是单条 agent 消息的长度上限。协议里最大的消息也就几十 KB，
// 1 MiB 足够宽松，同时挡住「用长度字段要求分配任意大内存」的畸形输入（原 D10）。
const maxMessageSize = 1 << 20

func readMessage(from io.Reader) ([]byte, error) {
	// In ssh-agent protocol, any message consists of:
	//
	//    uint32                   message length (network order)
	//    byte                     message type
	//    byte[message length - 1] message contents
	//
	// ref: https://tools.ietf.org/html/draft-miller-ssh-agent-04

	header := make([]byte, 4)
	if _, err := io.ReadFull(from, header); err != nil {
		return nil, err
	}

	n := binary.BigEndian.Uint32(header)
	if n > maxMessageSize {
		return nil, fmt.Errorf("message too large: %d bytes (limit %d)", n, maxMessageSize)
	}

	body := make([]byte, n)
	if _, err := io.ReadFull(from, body); err != nil {
		return nil, err
	}

	return append(header, body...), nil
}

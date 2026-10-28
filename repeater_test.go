package main

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"strconv"
	"testing"
	"time"
)

func setupDummyEnv(t *testing.T) string {
	t.Helper()
	log.SetOutput(io.Discard)
	requirePython(t)
	tmpDir := t.TempDir()
	// 保留原 PATH，便于假 powershell.exe 的 #!/usr/bin/env python3 可用；
	// tmpDir 放在最前，确保 powershellPath() 命中假 powershell.exe
	t.Setenv("PATH", tmpDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("DUMMY_PS_SRC_LEN", strconv.Itoa(len(repeaterPs1)))
	waitTimesBackup := waitTimes
	waitTimes = []time.Duration{0 * time.Second, 500 * time.Millisecond, 3 * time.Second}

	t.Cleanup(func() {
		waitTimes = waitTimesBackup
	})
	return tmpDir
}

func TestRepeaterNoPowerShell(t *testing.T) {
	setupDummyEnv(t)

	_, err := newRepeater(context.Background(), "/dummy/powershell.exe", "dummy-pipe-name")
	if err == nil || err.Error() != "failed to invoke PowerShell.exe 3 times; give up" {
		t.Errorf("should fail")
	}
}

func TestRepeaterBrokenPowerShell(t *testing.T) {
	tmpDir := setupDummyEnv(t)

	writeDummyPowerShell(t, tmpDir)
	t.Setenv("DUMMY_PS_MODE", "silent")

	_, err := newRepeater(context.Background(), powershellPath(), "dummy-pipe-name")
	if err == nil || err.Error() != "failed to invoke PowerShell.exe 3 times; give up" {
		t.Errorf("should fail")
	}
}

// TestRepeaterCtxCancelled：握手阶段必须能被 ctx 打断（原 D5）。
func TestRepeaterCtxCancelled(t *testing.T) {
	tmpDir := setupDummyEnv(t)

	writeDummyPowerShell(t, tmpDir)
	t.Setenv("DUMMY_PS_MODE", "silent") // 永不输出就绪字节

	// 把每轮等待拉长，确保只能靠 ctx 退出（setupDummyEnv 的 cleanup 会还原）
	waitTimes = []time.Duration{time.Hour, time.Hour, time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)

	start := time.Now()
	_, err := newRepeater(ctx, powershellPath(), "dummy-pipe-name")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("should return context.Canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("cancellation should be prompt, took %v", elapsed)
	}
}

func TestRepeaterNormal(t *testing.T) {
	tmpDir := setupDummyEnv(t)

	writeDummyPowerShell(t, tmpDir)
	t.Setenv("DUMMY_PS_MODE", "echo")

	rep, err := newRepeater(context.Background(), powershellPath(), "dummy-pipe-name")
	if err != nil {
		t.Fatalf("failed: %s", err)
	}
	defer rep.terminate()

	buf := make([]byte, len(repeaterPs1))
	_, err = io.ReadFull(rep.out, buf)
	if err != nil || string(buf) != repeaterPs1 {
		t.Errorf("does not work")
	}

	buf = make([]byte, 19)
	_, err = io.ReadFull(rep.out, buf)
	if err != nil || string(buf) != "\x00\x00\x00\x0fdummy-pipe-name" {
		t.Errorf("does not work: %s", string(buf))
	}

	_, err = rep.in.Write([]byte("Hello"))
	if err != nil {
		t.Fatal(err)
	}
	buf = make([]byte, 5)
	_, err = io.ReadFull(rep.out, buf)
	if err != nil || string(buf) != "Hello" {
		t.Errorf("does not work")
	}
}

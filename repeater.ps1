Function Log($msg) {
	$date = Get-Date -Format "yyyy/MM/dd HH:mm:ss"
	$host.ui.WriteErrorLine("[W] $date $msg")
}

Function ReadMessage($stream) {
	$buf = New-Object byte[] 4
	$offset = 0
	while ($offset -lt 4) {
		$n = $stream.Read($buf, $offset, 4 - $offset);
		if ($n -eq 0) {
			break
		}
		$offset += $n;
	}
	if ($offset -eq 4) {
		$len = (($buf[0] * 256 + $buf[1]) * 256 + $buf[2]) * 256 + $buf[3] + 4
		[Array]::Resize([ref]$buf, $len)
		while ($offset -lt $buf.Length) {
			$n = $stream.Read($buf, $offset, $buf.Length - $offset)
			if ($n -eq 0) {
				break
			}
			$offset += $n
		}
	}
	[Array]::Resize([ref]$buf, $offset)
	return $buf
}


# 命名管道连接超时（毫秒）。
# .NET 的 Connect() 默认没有超时：Bitwarden / ssh-agent.exe 没在运行时，管道实例
# 永远不出现，请求会一直静默卡死（实测 20 秒以上毫无反应，且 Go 侧看不到任何错误）。
$pipeConnectTimeout = 2000

# 链路级失败：长度 1、类型 0xFF（不在 SSH agent 协议内，真实 agent 不会发）。
# Go 侧拦下它且不向客户端回任何字节，于是 ssh 报「communication with agent failed」
# 这类传输错误，而不是「agent refused operation」。
Function SendLinkFailure($out) {
	$buf = [byte[]](0, 0, 0, 1, 0xff)
	$out.Write($buf, 0, $buf.Length)
}

# 管道是否存在于 Windows 的管道命名空间。
# 有了它就能跟 Windows 侧 ssh.exe 一致：agent 没在运行时立刻失败，
# 而不是干等 Connect 超时（Windows 的 CreateFile 也是立即报错的）。
Function PipeExists($name) {
	Try {
		$target = "\\.\pipe\$name"
		foreach ($p in [System.IO.Directory]::GetFiles("\\.\pipe\")) {
			if ($p -ieq $target) {
				return $true
			}
		}
		return $false
	}
	Catch {
		# 列不出管道时不要误伤正常流程，交给 Connect 自行判断
		return $true
	}
}

# 带超时的连接；失败只记日志并返回 $false，由调用方回 SSH_AGENT_FAILURE。
Function ConnectPipe($pipe, $name) {
	Try {
		if (-not (PipeExists $name)) {
			Log "[W] named pipe '$name' does not exist (is Bitwarden / ssh-agent running?)"
			return $false
		}

		$pipe.Connect($pipeConnectTimeout)
		return $true
	}
	Catch {
		Log "[W] cannot connect to named pipe '$name': $($_.Exception.Message)"
		return $false
	}
}

Function MainLoop {
	Try {
		$ignoreOpenSSHExtensions = $false
		Try {
			$sshAgentVersion = (Get-Command -CommandType Application ssh-agent.exe -ErrorAction Stop)[0].Version
			$ignoreOpenSSHExtensions = ($sshAgentVersion.Major -le 8 -and $sshAgentVersion.Minor -lt 9)
			Log "ssh-agent.exe version: $($sshAgentVersion.ToString()) (ignoreOpenSSHExtensions: $ignoreOpenSSHExtensions)"
		}
		Catch {
			$ignoreOpenSSHExtensions = $true
		}

		$ssh_client_in = [console]::OpenStandardInput()
		$ssh_client_out = [console]::OpenStandardOutput()

		$ver = $PSVersionTable["PSVersion"]
		$ssh_client_out.WriteByte(0xff)
		Log "ready: PSVersion $ver"

		$buf = ReadMessage $ssh_client_in
		$pipename = [System.Text.Encoding]::UTF8.GetString($buf[4..$buf.Length])
		Log "[W] named pipe: $pipename"

		while ($true) {
			Try {
				$buf = ReadMessage $ssh_client_in
				if ($buf.Length -lt 5) {
					# stdin 已关闭（daemon 正在退出）
					Log "stdin closed; exit"
					break
				}
				if ($ignoreOpenSSHExtensions -and $buf.Length -gt 4 -and $buf[4] -eq 0x1b) {
					$buf = [byte[]](0, 0, 0, 1, 6)
					$ssh_client_out.Write($buf, 0, $buf.Length)
					Log "[W] return dummy for OpenSSH ext."
					Continue
				}
				$ssh_agent = New-Object System.IO.Pipes.NamedPipeClientStream ".", $pipename, InOut
				if (-not (ConnectPipe $ssh_agent $pipename)) {
					SendLinkFailure $ssh_client_out
					Continue
				}
				Log "[W] named pipe: connected"
				$ssh_agent.Write($buf, 0, $buf.Length)
				Log "[L] -> [W] -> ssh-agent.exe ($($buf.Length) B)"
				$buf = ReadMessage $ssh_agent
				$ssh_client_out.Write($buf, 0, $buf.Length)
				Log "[L] <- [W] <- ssh-agent.exe ($($buf.Length) B)"
			}
			Catch {
				# 任何意外都回链路级失败，并让进程继续活着（下个请求重新连）
				Log "error: $($_.Exception.Message)"
				SendLinkFailure $ssh_client_out
			}
			Finally {
				if ($null -ne $ssh_agent) {
					$ssh_agent.Dispose()
					Log "[W] named pipe: disconnected"
					$ssh_agent = $null
				}
			}
		}
	}
	Finally {
		$host.ui.WriteErrorLine("wsl2-ssh-agent.ps1: terminated")
	}
}

MainLoop

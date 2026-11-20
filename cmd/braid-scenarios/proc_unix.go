//go:build unix

package main

import (
	"os"
	"os/exec"
	"syscall"
)

// setGroup starts the process in its own process group, so killGroup also ends its children.
func setGroup(c *exec.Cmd) { c.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }

func killGroup(p *os.Process) { syscall.Kill(-p.Pid, syscall.SIGKILL) }

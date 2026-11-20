//go:build unix

package main

import "syscall"

func gone(pid int) bool { return syscall.Kill(pid, 0) == syscall.ESRCH }

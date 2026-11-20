package main

import "syscall"

func gone(pid int) bool {
	h, err := syscall.OpenProcess(syscall.PROCESS_QUERY_INFORMATION, false, uint32(pid))
	if err != nil {
		return true
	}
	defer syscall.CloseHandle(h)
	var code uint32
	// 259 is STILL_ACTIVE.
	return syscall.GetExitCodeProcess(h, &code) == nil && code != 259
}

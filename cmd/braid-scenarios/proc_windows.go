package main

import (
	"os"
	"os/exec"
)

// setGroup does nothing on Windows: a braid-peer starts no children, so killGroup ends the process alone.
func setGroup(*exec.Cmd) {}

func killGroup(p *os.Process) { p.Kill() }

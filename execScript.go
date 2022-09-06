package main

import (
	"context"
	//"bytes"
	"log"
	//"os"
	logger "github.com/wonderivan/logger"
	"os/exec"
	"strings"
	"syscall"
)

var cancelFunc context.CancelFunc

func ExecExternalScript(cmdTxt string) (string, string, error) {
	//logger.Debug("Start execExternalScript! ")
	//log.Printf("           ====================      start execExternalScript            =============")

	logger.Debug("start exec %s.", cmdTxt)

	ctx, cancel := context.WithCancel(context.Background())
	cancelFunc = cancel

	//log.Printf("cmdTxt: %v", cmdTxt)

	cmd := exec.CommandContext(ctx, "bash", "-c", cmdTxt)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	//cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Start()

	pgid, _ := syscall.Getpgid(cmd.Process.Pid)
	//var killFunc func() error
	killFunc := func() error {
		log.Printf("current pgid [%v]", pgid)
		return syscall.Kill(-pgid, syscall.SIGKILL) // note the minus sign
	}
	log.Printf("%v", killFunc())

	if err := cmd.Wait(); err != nil {
		log.Printf("script cmd.wait(), err: %v", err)
	} else {
		//log.Printf("script finished!")
	}

	//go func() {
	//	if err := cmd.Wait(); err != nil {
	//		fmt.Printf("Child process %d exit with err: %v\n", cmd.Process.Pid, err)
	//	}
	//}()
	//time.Sleep(1 * time.Second)
	//cmd.Process.Kill()
	log.Printf("              =================      execExternalScript finished" +
		"         ================")

	return stdout.String(), stderr.String(), nil
}

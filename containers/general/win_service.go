//go:build windows
// +build windows

package main

import (
	"fmt"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/debug"
	"golang.org/x/sys/windows/svc/mgr"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type serviceInstance struct {
	args []string
}

func serviceMode(thisExe string, action string, args []string) error {
	if action == "-debug" {
		return runDebugService("test-service", args)
	}
	components := strings.SplitN(action, ":", 2)
	if len(components) < 2 {
		return fmt.Errorf("usage: [-install:svcName | -remove:svcName | -run:svcName | debug]")
	}
	action = components[0]
	svcName := components[1]

	isInService, err := svc.IsWindowsService()
	if err != nil {
		return err
	}
	if isInService {
		return runService(svcName, args)
	}
	if action == "-install" {
		thisExe, err = filepath.Abs(thisExe)
		if err != nil {
			return err
		}
		return installService(thisExe, svcName, args)
	} else if action == "-remove" {
		return removeService(svcName)
	} else {
		return fmt.Errorf("unknown action: %s", action)
	}
}

func runService(svcName string, args []string) error {
	if err := svc.Run(svcName, &serviceInstance{
		args: args,
	}); err != nil {
		return err
	}

	return nil
}

func runDebugService(svcName string, args []string) error {
	if err := debug.Run(svcName, &serviceInstance{
		args: args,
	}); err != nil {
		return err
	}

	return nil
}

func installService(thisExe string, svcName string, args []string) error {
	m, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer m.Disconnect()
	s, err := m.OpenService(svcName)
	if err == nil {
		s.Close()
		return fmt.Errorf("service %s already exists", svcName)
	}
	args = append([]string{fmt.Sprintf("-run:%s", svcName)}, args...)
	s, err = m.CreateService(svcName, thisExe, mgr.Config{
		StartType:    mgr.StartAutomatic,
		ErrorControl: mgr.ErrorNormal,
		Description:  "LimaCharlie Adapter",
		DisplayName:  fmt.Sprintf("LimaCharlie Adapter - %s", svcName),
	}, args...)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := s.Start(args...); err != nil {
		return err
	}
	log("Service %s installed.", svcName)
	return nil
}

func removeService(svcName string) error {
	m, err := mgr.Connect()
	if err != nil {
		return err
	}
	defer m.Disconnect()
	s, err := m.OpenService(svcName)
	if err != nil {
		return fmt.Errorf("service %s is not installed", svcName)
	}
	defer s.Close()
	if status, err := s.Control(svc.Stop); err != nil {
		logError("Control.Stop(): %v / %v", err, status)
	}

	if err := s.Delete(); err != nil {
		return err
	}
	log("Service %s uninstalled.", svcName)
	return nil
}

func (m *serviceInstance) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (ssec bool, errno uint32) {
	acceptedControls := svc.AcceptStop | svc.AcceptShutdown
	changes <- svc.Status{State: svc.StartPending, Accepts: acceptedControls}
	args = m.args
	method, configsToRun, err := parseConfigs(args)
	if err != nil {
		saveErrorOnDisk(logError("parseConfigs(): %v", err))
		return false, 1
	}

	// Running as a Service we are not guaranteed that the network
	// stack will be up and running yet, give it a few moments.
	retries := 0
	for retries < 5 {
		if _, err := net.LookupIP("api.limacharlie.io"); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
		retries++
	}

	if len(configsToRun) == 0 {
		logError("no configs to run")
		os.Exit(1)
		return
	}
	clients := []USPClient{}
	chRunnings := make(chan struct{})
	for _, config := range configsToRun {
		log("starting adapter: %s", method)
		client, chRunning, err := runAdapter(method, *config)
		if err != nil {
			saveErrorOnDisk(logError("runAdapter(): %v", err))
			return false, 1
		}
		clients = append(clients, client)
		go func() {
			<-chRunning
			chRunnings <- struct{}{}
		}()
	}

	wg := sync.WaitGroup{}

	go func() {
		for c := range r {
			if c.Cmd == svc.Stop || c.Cmd == svc.Shutdown {
				changes <- svc.Status{State: svc.StopPending, Accepts: acceptedControls}
				for _, client := range clients {
					client.Close()
				}
			} else if c.Cmd == svc.Interrogate {
				changes <- c.CurrentStatus
			} else {
				logError("unexpected control request: #%d", c)
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-chRunnings
		changes <- svc.Status{State: svc.Stopped, Accepts: acceptedControls}
	}()

	changes <- svc.Status{State: svc.Running, Accepts: acceptedControls}

	wg.Wait()
	return false, 0
}

func saveErrorOnDisk(err string) {
	f, _ := os.Create("lc_adapter.log")
	f.Write([]byte(err))
	f.Close()
}

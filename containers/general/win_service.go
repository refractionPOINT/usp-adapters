//go:build windows
// +build windows

package main

import (
	"fmt"
	"golang.org/x/sys/windows/svc"
	"golang.org/x/sys/windows/svc/mgr"
	"strings"
	"sync"
)

type serviceInstance struct {
	args []string
}

func serviceMode(thisExe string, action string, args []string) error {
	components := strings.SplitN(action, ":", 2)
	if len(components) < 2 {
		return fmt.Errorf("usage: [-install:svcName | -remove:svcName | -run:svcName]")
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
	err = s.Delete()
	if err != nil {
		return err
	}
	return nil
}

func (m *serviceInstance) Execute(args []string, r <-chan svc.ChangeRequest, changes chan<- svc.Status) (ssec bool, errno uint32) {
	method, runtimeConfigs, configs, err := parseConfigs(args)
	if err != nil {
		logError("parseConfigs(): %v", err)
		return false, 1
	}
	client, chRunning, err := runAdapter(method, *runtimeConfigs, *configs)
	if err != nil {
		logError("runAdapter(): %v", err)
		return false, 1
	}
	defer client.Close()

	wg := sync.WaitGroup{}

	go func() {
		for c := range r {
			if c.Cmd == svc.Stop || c.Cmd == svc.Shutdown {
				changes <- svc.Status{State: svc.StopPending}
				client.Close()
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
		<-chRunning
		changes <- svc.Status{State: svc.Stopped}
	}()

	wg.Wait()
	return false, 0
}

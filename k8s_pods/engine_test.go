//go:build windows || darwin || linux || solaris || netbsd || openbsd || freebsd
// +build windows darwin linux solaris netbsd openbsd freebsd

package usp_k8s_pods

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
)

const (
	testDir        = "testdir"
	pod1           = "monitoring_alertmanager-prometheus-kube-prometheus-alertmanager-0_5fd1df1d-b484-435d-b497-deb676db1614"
	pod1container1 = "alertmanager"
	pod1container2 = "config-reloader"
	pod2           = "limacharlie_endpoint-0_hdaadf1d-bbbb-435d-b497-cccc76db1614"
	pod2container1 = "endpoint"
	podNot         = "limacharlie_notagoodpod-0_hdaadf1d-bbbb-435d-b497-cccc76db1614"
)

func TestEngine(t *testing.T) {
	os.RemoveAll(testDir)
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatal(err)
	}
	k, err := NewK8sLogProcessor(testDir, uspclient.ClientOptions{
		DebugLog: func(msg string) {
			fmt.Println("DEBUG", msg)
		},
		OnWarning: func(msg string) {
			fmt.Println("WARN", msg)
		},
		OnError: func(err error) {
			fmt.Println("ERROR", err)
		},
	}, runtimeOptions{
		excludePods: regexp.MustCompile(`.*notagoodpod.*`),
	})
	if err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(1 * time.Second)
		if err := os.MkdirAll(path.Join(testDir, pod1, pod1container1), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(path.Join(testDir, pod1, pod1container1, "0.log"), []byte("test1\ntest2\ntest3\n"), 0644); err != nil {
			t.Error(err)
		}
		time.Sleep(1 * time.Second)
		if err := os.WriteFile(path.Join(testDir, pod1, pod1container1, "1.log"), []byte("test4\ntest5\ntest6\n"), 0644); err != nil {
			t.Error(err)
		}

		if err := os.MkdirAll(path.Join(testDir, pod1, pod1container2), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(path.Join(testDir, pod1, pod1container2, "0.log"), []byte("test1\ntest2\ntest3\n"), 0644); err != nil {
			t.Error(err)
		}
		time.Sleep(1 * time.Second)
		if err := os.MkdirAll(path.Join(testDir, podNot, pod2container1), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(path.Join(testDir, podNot, pod2container1, "0.log"), []byte("test1\ntest2\ntest3\n"), 0644); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(path.Join(testDir, pod1, pod1container2, "1.log"), []byte("test4\ntest5\ntest6\n"), 0644); err != nil {
			t.Error(err)
		}
		if err := os.MkdirAll(path.Join(testDir, pod2, pod2container1), 0755); err != nil {
			t.Error(err)
		}
		if err := os.WriteFile(path.Join(testDir, pod2, pod2container1, "0.log"), []byte("test1\ntest2\ntest3\n"), 0644); err != nil {
			t.Error(err)
		}
		time.Sleep(1 * time.Second)
		if err := os.WriteFile(path.Join(testDir, pod2, pod2container1, "1.log"), []byte("test4\ntest5\ntest6\n"), 0644); err != nil {
			t.Error(err)
		}
		if err := os.RemoveAll(path.Join(testDir, pod1)); err != nil {
			t.Error(err)
		}

		time.Sleep(5 * time.Second)
		k.Close()

		if err := os.RemoveAll(testDir); err != nil {
			t.Error(err)
		}
	}()

	pod1container1Count := 0
	pod1container2Count := 0
	pod2container1Count := 0

	for l := range k.Lines() {
		if l.Entity.ContainerName == pod1container1 {
			pod1container1Count++
		} else if l.Entity.ContainerName == pod1container2 {
			pod1container2Count++
		} else if l.Entity.ContainerName == pod2container1 {
			pod2container1Count++
		} else {
			t.Errorf("unknown line: %+v", l)
		}
	}
	if pod1container1Count != 6 {
		t.Errorf("pod1container1Count: %d", pod1container1Count)
	}
	if pod1container2Count != 6 {
		t.Errorf("pod1container2Count: %d", pod1container2Count)
	}
	if pod2container1Count != 6 {
		t.Errorf("pod2container1Count: %d", pod2container1Count)
	}

	wg.Wait()
}

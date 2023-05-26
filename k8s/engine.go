package usp_k8s

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/refractionPOINT/go-uspclient"

	"github.com/fsnotify/fsnotify"
)

var k8sLogFileNamePattern = regexp.MustCompile("(.+)_(.+)_(.+)/(.+)/.+")

type K8sLogProcessor struct {
	options uspclient.ClientOptions
	root    string

	wg     sync.WaitGroup
	chStop chan struct{}

	rootWatcher *fsnotify.Watcher

	chNewFiles chan k8sFileMtd

	currentlyRunning map[k8sEntity]entityRuntimeState
}

type k8sFileMtd struct {
	FileName            string    `json:"file_name"`
	IsProcessEntireFile bool      `json:"-"`
	Entity              k8sEntity `json:"entity"`
}

type k8sEntity struct {
	Namespace     string `json:"namespace"`
	PodName       string `json:"pod_name"`
	PodID         string `json:"pod_id"`
	ContainerName string `json:"container_name"`
}

type entityRuntimeState struct {
	Stop chan struct{}
	Done chan struct{}
}

func NewK8sLogProcessor(root string, cOpt uspclient.ClientOptions) (*K8sLogProcessor, error) {
	klp := &K8sLogProcessor{
		options:    cOpt,
		root:       root,
		chStop:     make(chan struct{}),
		chNewFiles: make(chan k8sFileMtd),

		currentlyRunning: make(map[k8sEntity]entityRuntimeState),
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	klp.rootWatcher = w
	if err := klp.rootWatcher.Add(root); err != nil {
		return nil, err
	}
	klp.wg.Add(1)
	go klp.watchForPods()

	klp.wg.Add(1)
	go klp.watchForPods()

	return klp, nil
}

func (klp *K8sLogProcessor) Close() error {
	close(klp.chStop)
	klp.rootWatcher.Close()
	klp.wg.Wait()
	return nil
}

func (klp *K8sLogProcessor) watchForPods() {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s root watcher stopped")

	for {
		select {
		case event, ok := <-klp.rootWatcher.Events:
			if !ok {
				return
			}
			if event.Op != fsnotify.Create {
				continue
			}
			klp.wg.Add(1)
			go klp.watchPod(fmt.Sprintf("%s/%s", klp.root, event.Name))
		case err, ok := <-klp.rootWatcher.Errors:
			if !ok {
				return
			}
			klp.options.OnError(err)
		case <-klp.chStop:
			return
		}
	}
}

func (klp *K8sLogProcessor) watchPod(podName string) {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s pod watcher stopped: " + podName)

	klp.options.DebugLog("k8s pod watcher started: " + podName)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		klp.options.OnError(err)
		return
	}
	defer w.Close()
	if err := w.Add(podName); err != nil {
		klp.options.OnError(err)
		return
	}

	running := map[string]chan struct{}{}

	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			containerPath := fmt.Sprintf("%s/%s", podName, event.Name)

			if event.Op == fsnotify.Remove {
				klp.options.DebugLog("k8s container removed: " + containerPath)
				close(running[containerPath])
				delete(running, containerPath)
				continue
			}
			if event.Op != fsnotify.Create {
				continue
			}

			running[containerPath] = make(chan struct{})
			klp.wg.Add(1)
			go klp.watchContainer(containerPath, running[containerPath])
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			klp.options.OnError(err)
		case <-klp.chStop:
			return
		}
	}
}

func (klp *K8sLogProcessor) watchContainer(containerPath string, chStop chan struct{}) {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s container watcher stopped: " + containerPath)

	klp.options.DebugLog("k8s container watcher started: " + containerPath)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		klp.options.OnError(err)
		return
	}
	defer w.Close()
	if err := w.Add(containerPath); err != nil {
		klp.options.OnError(err)
		return
	}

	running := map[string]chan struct{}{}

	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			logPath := fmt.Sprintf("%s/%s", containerPath, event.Name)

			if event.Op == fsnotify.Remove {
				klp.options.DebugLog("k8s log removed: " + logPath)
				close(running[logPath])
				delete(running, logPath)
				continue
			}
			if event.Op != fsnotify.Create {
				continue
			}

			components := k8sLogFileNamePattern.FindStringSubmatch(strings.TrimPrefix(logPath, "/"))
			if len(components) != 5 {
				klp.options.DebugLog("k8s file name does not match pattern: " + logPath)
				continue
			}
			k8sMtd := k8sFileMtd{
				FileName: logPath,
				Entity: k8sEntity{
					Namespace:     components[1],
					PodName:       components[2],
					PodID:         components[3],
					ContainerName: components[4],
				},
			}
			if k8sMtd.Entity.Namespace == "" || k8sMtd.Entity.PodName == "" || k8sMtd.Entity.PodID == "" || k8sMtd.Entity.ContainerName == "" {
				klp.options.DebugLog("k8s file name does not match pattern: " + logPath)
				continue
			}

			running[logPath] = make(chan struct{})

			klp.wg.Add(1)
			go klp.processFile(k8sMtd, running[logPath])
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			klp.options.OnError(err)
		case <-chStop:
			return
		}
	}
}

func (klp *K8sLogProcessor) processFile(file k8sFileMtd, chStop chan struct{}) {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s processing file done: " + file.FileName)

	klp.options.DebugLog("k8s processing file started: " + file.FileName)
}

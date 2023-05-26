package usp_k8s

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/refractionPOINT/go-uspclient"

	"github.com/fsnotify/fsnotify"
	"github.com/nxadm/tail"
)

var k8sPodPattern = regexp.MustCompile("(.+)_(.+)_(.+)")

type K8sLogProcessor struct {
	options uspclient.ClientOptions
	root    string

	wg     sync.WaitGroup
	chStop chan struct{}

	rootWatcher *fsnotify.Watcher

	chNewFiles chan k8sFileMtd

	chLines chan K8sLogLine
}

type k8sFileMtd struct {
	FileName            string    `json:"file_name"`
	IsProcessEntireFile bool      `json:"-"`
	Entity              K8sEntity `json:"entity"`
}

type K8sEntity struct {
	Namespace     string `json:"namespace"`
	PodName       string `json:"pod_name"`
	PodID         string `json:"pod_id"`
	ContainerName string `json:"container_name"`
}

type K8sLogLine struct {
	Entity K8sEntity `json:"entity"`
	Line   string    `json:"line"`
}

func NewK8sLogProcessor(root string, cOpt uspclient.ClientOptions) (*K8sLogProcessor, error) {
	klp := &K8sLogProcessor{
		options:    cOpt,
		root:       root,
		chStop:     make(chan struct{}),
		chNewFiles: make(chan k8sFileMtd),
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

	return klp, nil
}

func (klp *K8sLogProcessor) Lines() chan K8sLogLine {
	return klp.chLines
}

func (klp *K8sLogProcessor) Close() error {
	close(klp.chStop)
	klp.rootWatcher.Close()
	close(klp.chLines)
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
			components := k8sPodPattern.FindStringSubmatch(strings.TrimPrefix(event.Name, "/"))
			if len(components) != 4 {
				klp.options.DebugLog("k8s pod name does not match pattern: " + event.Name)
				continue
			}
			mtd := k8sFileMtd{
				FileName: fmt.Sprintf("%s/%s", klp.root, event.Name),
				Entity: K8sEntity{
					Namespace: components[1],
					PodName:   components[2],
					PodID:     components[3],
				},
			}
			if mtd.Entity.Namespace == "" || mtd.Entity.PodName == "" || mtd.Entity.PodID == "" {
				klp.options.DebugLog("k8s pod name does not match pattern: " + event.Name)
				continue
			}
			klp.wg.Add(1)
			go klp.watchPod(mtd)
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

func (klp *K8sLogProcessor) watchPod(mtd k8sFileMtd) {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s pod watcher stopped: " + mtd.FileName)

	klp.options.DebugLog("k8s pod watcher started: " + mtd.FileName)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		klp.options.OnError(err)
		return
	}
	defer w.Close()
	if err := w.Add(mtd.FileName); err != nil {
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
			containerPath := fmt.Sprintf("%s/%s", mtd.FileName, event.Name)

			if event.Op == fsnotify.Remove {
				klp.options.DebugLog("k8s container removed: " + containerPath)
				close(running[containerPath])
				delete(running, containerPath)
				continue
			}
			if event.Op != fsnotify.Create {
				continue
			}

			newMtd := mtd
			newMtd.FileName = containerPath
			newMtd.Entity.ContainerName = event.Name

			running[containerPath] = make(chan struct{})
			klp.wg.Add(1)
			go klp.watchContainer(newMtd, running[containerPath])
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			klp.options.OnError(err)
		case <-klp.chStop:
			for _, ch := range running {
				close(ch)
			}
			return
		}
	}
}

func (klp *K8sLogProcessor) watchContainer(mtd k8sFileMtd, chStop chan struct{}) {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s container watcher stopped: " + mtd.FileName)

	klp.options.DebugLog("k8s container watcher started: " + mtd.FileName)

	w, err := fsnotify.NewWatcher()
	if err != nil {
		klp.options.OnError(err)
		return
	}
	defer w.Close()
	if err := w.Add(mtd.FileName); err != nil {
		klp.options.OnError(err)
		return
	}

	var curFile *tail.Tail

	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			logPath := fmt.Sprintf("%s/%s", mtd.FileName, event.Name)

			if event.Op == fsnotify.Remove {
				klp.options.DebugLog("k8s log removed: " + logPath)
				curFile.StopAtEOF()
				continue
			}
			if event.Op != fsnotify.Create {
				continue
			}

			newMtd := mtd
			newMtd.FileName = logPath

			curFile, err = tail.TailFile(logPath, tail.Config{
				Follow:        true,
				ReOpen:        true,
				MustExist:     true,
				CompleteLines: true,
			})
			if err != nil {
				klp.options.OnError(err)
				continue
			}

			klp.wg.Add(1)
			go klp.processFile(newMtd, curFile)
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			klp.options.OnError(err)
		case <-chStop:
			curFile.StopAtEOF()
			return
		}
	}
}

func (klp *K8sLogProcessor) processFile(mtd k8sFileMtd, file *tail.Tail) {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s processing file done: " + mtd.FileName)

	klp.options.DebugLog("k8s processing file started: " + mtd.FileName)

	for line := range file.Lines {
		if line.Err != nil {
			klp.options.OnError(line.Err)
			return
		}
		klp.chLines <- K8sLogLine{
			Entity: mtd.Entity,
			Line:   line.Text,
		}
	}
}

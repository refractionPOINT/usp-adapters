package usp_k8s_pods

import (
	"fmt"
	"os"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/refractionPOINT/go-uspclient"

	"github.com/fsnotify/fsnotify"
	"github.com/nxadm/tail"
)

var k8sPodPattern = regexp.MustCompile(".+/(.+)_(.+)_(.+)")

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
		chLines:    make(chan K8sLogLine),
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("fsnotify.NewWatcher(): %v", err)
	}
	klp.rootWatcher = w
	klp.options.DebugLog("k8s root watcher created: " + root)
	if err := klp.rootWatcher.Add(root); err != nil {
		return nil, fmt.Errorf("rootWatcher.Add(): %v", err)
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

	// Initialize all containers already present.
	running := map[string]chan struct{}{}
	if files, err := os.ReadDir(klp.root); err == nil {
		for _, file := range files {
			if !file.IsDir() {
				continue
			}
			podPath := path.Join(klp.root, file.Name())
			if _, ok := running[podPath]; ok {
				continue
			}
			components := k8sPodPattern.FindStringSubmatch(podPath)
			if len(components) != 4 {
				klp.options.DebugLog("k8s existing pod name does not match pattern: " + podPath)
				continue
			}
			mtd := k8sFileMtd{
				FileName: podPath,
				Entity: K8sEntity{
					Namespace: components[1],
					PodName:   components[2],
					PodID:     components[3],
				},
			}
			if mtd.Entity.Namespace == "" || mtd.Entity.PodName == "" || mtd.Entity.PodID == "" {
				klp.options.DebugLog("k8s existing pod name does not match pattern: " + podPath)
				continue
			}
			klp.wg.Add(1)
			go klp.watchPod(mtd)
		}
	}

	for {
		select {
		case event, ok := <-klp.rootWatcher.Events:
			if !ok {
				return
			}
			if event.Op != fsnotify.Create {
				continue
			}
			if _, ok := running[event.Name]; ok {
				continue
			}
			components := k8sPodPattern.FindStringSubmatch(event.Name)
			if len(components) != 4 {
				klp.options.DebugLog("k8s pod name does not match pattern: " + event.Name)
				continue
			}
			mtd := k8sFileMtd{
				FileName: event.Name,
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
			klp.options.DebugLog("k8s root watcher error: " + err.Error())
			if !ok {
				return
			}
			klp.options.OnError(fmt.Errorf("error watching root %s: %v", klp.root, err))
		case <-klp.chStop:
			klp.options.DebugLog("k8s root watcher stopping")
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
		klp.options.OnError(fmt.Errorf("error watching pod %s: %v", mtd.FileName, err))
		return
	}
	defer w.Close()
	if err := w.Add(mtd.FileName); err != nil {
		klp.options.OnError(fmt.Errorf("error adding pod watch %s: %v", mtd.FileName, err))
		return
	}

	running := map[string]chan struct{}{}
	defer func() {
		klp.options.DebugLog("signaling containers to stop: " + mtd.FileName)
		for _, ch := range running {
			close(ch)
		}
	}()

	// Initialize all containers already present.
	if files, err := os.ReadDir(mtd.FileName); err == nil {
		for _, file := range files {
			if !file.IsDir() {
				continue
			}
			containerPath := path.Join(mtd.FileName, file.Name())
			if _, ok := running[containerPath]; ok {
				continue
			}
			nameComponents := strings.Split(file.Name(), "/")
			newMtd := mtd
			newMtd.FileName = containerPath
			newMtd.Entity.ContainerName = nameComponents[len(nameComponents)-1]

			running[containerPath] = make(chan struct{})
			klp.wg.Add(1)
			go klp.watchContainer(newMtd, running[containerPath])
		}
	}

	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			containerPath := event.Name

			if event.Op == fsnotify.Remove {
				if event.Name == mtd.FileName {
					klp.options.DebugLog("k8s pod watcher stopping")
					return
				}
				klp.options.DebugLog("k8s container removed: " + containerPath)
				close(running[containerPath])
				delete(running, containerPath)
				continue
			}
			if event.Op != fsnotify.Create {
				continue
			}

			// There could be a race condition with init, ignore.
			if _, ok := running[containerPath]; ok {
				continue
			}

			nameComponents := strings.Split(event.Name, "/")
			newMtd := mtd
			newMtd.FileName = containerPath
			newMtd.Entity.ContainerName = nameComponents[len(nameComponents)-1]

			running[containerPath] = make(chan struct{})
			klp.wg.Add(1)
			go klp.watchContainer(newMtd, running[containerPath])
		case err, ok := <-w.Errors:
			if !ok {
				klp.options.DebugLog("k8s pod watcher stopping")
				return
			}
			klp.options.OnError(fmt.Errorf("error from pod watch %s: %v", mtd.FileName, err))
		case <-klp.chStop:
			klp.options.DebugLog("k8s pod watcher stopping")
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
		klp.options.OnError(fmt.Errorf("error watching container %s: %v", mtd.FileName, err))
		return
	}
	defer w.Close()
	if err := w.Add(mtd.FileName); err != nil {
		klp.options.OnError(fmt.Errorf("error adding container watch %s: %v", mtd.FileName, err))
		return
	}

	var curFile *tail.Tail
	currentLog := ""

	// If logs are already present, start tailing the latest.
	if files, err := os.ReadDir(mtd.FileName); err == nil {
		latestLog := ""
		latestMod := time.Time{}
		for _, file := range files {
			if file.IsDir() {
				continue
			}
			info, err := file.Info()
			if err != nil {
				continue
			}
			mod := info.ModTime()
			if mod.After(latestMod) {
				latestLog = file.Name()
				latestMod = mod
			}
		}
		if latestLog != "" {
			klp.options.DebugLog("k8s container log priming: " + path.Join(mtd.FileName, latestLog))
			newMtd := mtd
			logPath := path.Join(mtd.FileName, latestLog)
			newMtd.FileName = logPath

			curFile, err = tail.TailFile(logPath, tail.Config{
				Follow:        true,
				ReOpen:        true,
				MustExist:     true,
				CompleteLines: true,
			})
			if err != nil {
				klp.options.OnError(fmt.Errorf("error tailing %s: %v", logPath, err))
			} else {
				klp.wg.Add(1)
				go klp.processFile(newMtd, curFile)
				currentLog = logPath
			}
		}
	}

	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				klp.options.DebugLog("k8s container watcher stopping: " + mtd.FileName)
				return
			}
			logPath := event.Name

			if event.Op == fsnotify.Remove {
				klp.options.DebugLog("k8s log removed: " + logPath)
				curFile.StopAtEOF()
				curFile.Cleanup()
				continue
			}
			if event.Op != fsnotify.Create {
				continue
			}

			// If this is already the log we're tracking, it's likely
			// a race condition with init.
			if logPath == currentLog {
				continue
			}

			// If a tail is currently running, it likely means that
			// the log is cycling.
			if curFile != nil {
				klp.options.DebugLog("k8s log cycled: " + logPath)
				curFile.StopAtEOF()
				curFile.Cleanup()
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
				klp.options.OnError(fmt.Errorf("error tailing log %s: %v", logPath, err))
				continue
			}

			klp.wg.Add(1)
			go klp.processFile(newMtd, curFile)
		case err, ok := <-w.Errors:
			if !ok {
				klp.options.DebugLog("k8s container watcher stopping: " + mtd.FileName)
				return
			}
			klp.options.OnError(fmt.Errorf("error from container watch %s: %v", mtd.FileName, err))
		case <-chStop:
			klp.options.DebugLog("k8s container watcher stopping: " + mtd.FileName)
			curFile.StopAtEOF()
			curFile.Cleanup()
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

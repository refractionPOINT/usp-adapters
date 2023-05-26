package usp_k8s

import (
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

	watcher *fsnotify.Watcher

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
	klp.watcher = w
	if err := klp.watcher.Add(root); err != nil {
		return nil, err
	}
	klp.wg.Add(1)
	go klp.watchChanges()

	klp.wg.Add(1)
	go klp.watchNewFiles()

	return klp, nil
}

func (klp *K8sLogProcessor) Close() error {
	close(klp.chStop)
	klp.watcher.Close()
	klp.wg.Wait()
	return nil
}

func (klp *K8sLogProcessor) watchChanges() {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s file watcher stopped")
	defer close(klp.chNewFiles)

	for {
		select {
		case event, ok := <-klp.watcher.Events:
			if !ok {
				return
			}
			if event.Op != fsnotify.Create {
				continue
			}
			klp.chNewFiles <- k8sFileMtd{
				FileName: event.Name,
			}
			klp.options.DebugLog("k8s file watcher event: " + event.Name)
		case err, ok := <-klp.watcher.Errors:
			if !ok {
				return
			}
			klp.options.OnError(err)
		case <-klp.chStop:
			return
		}
	}
}

func (klp *K8sLogProcessor) watchNewFiles() {
	defer klp.wg.Done()
	for file := range klp.chNewFiles {
		klp.options.DebugLog("k8s processing file: " + file.FileName)

		components := k8sLogFileNamePattern.FindStringSubmatch(strings.TrimPrefix(file.FileName, "/"))
		if len(components) != 5 {
			klp.options.DebugLog("k8s file name does not match pattern: " + file.FileName)
			continue
		}
		k8sMtd := k8sFileMtd{
			FileName: file.FileName,
			Entity: k8sEntity{
				Namespace:     components[1],
				PodName:       components[2],
				PodID:         components[3],
				ContainerName: components[4],
			},
		}
		if k8sMtd.Entity.Namespace == "" || k8sMtd.Entity.PodName == "" || k8sMtd.Entity.PodID == "" || k8sMtd.Entity.ContainerName == "" {
			klp.options.DebugLog("k8s file name does not match pattern: " + file.FileName)
			continue
		}

		klp.wg.Add(1)
		go klp.processFile(k8sMtd)
	}
}

func (klp *K8sLogProcessor) processFile(file k8sFileMtd) {
	defer klp.wg.Done()
	defer klp.options.DebugLog("k8s processing file done: " + file.FileName)
}

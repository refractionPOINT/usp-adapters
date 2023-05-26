package usp_k8s

import (
	"sync"

	"github.com/refractionPOINT/go-uspclient"

	"github.com/fsnotify/fsnotify"
)

type K8sLogProcessor struct {
	options uspclient.ClientOptions
	root    string

	wg     sync.WaitGroup
	chStop chan struct{}

	watcher *fsnotify.Watcher
}

func NewK8sLogProcessor(root string, cOpt uspclient.ClientOptions) (*K8sLogProcessor, error) {
	klp := &K8sLogProcessor{
		options: cOpt,
		root:    root,
		chStop:  make(chan struct{}),
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

	for {
		select {
		case event, ok := <-klp.watcher.Events:
			if !ok {
				return
			}
			if event.Op != fsnotify.Create {
				continue
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

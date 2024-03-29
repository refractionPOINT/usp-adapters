//go:build aix
// +build aix

package usp_k8s_pods

import "errors"

// Dummy noop file to build when the platform
// is _not_ supported.

type K8sPodsAdapter struct{}

func NewK8sPodsAdapter(conf K8sPodsConfig) (*K8sPodsAdapter, chan struct{}, error) {
	return nil, nil, errors.New("k8s_pods collection not supported on this platform")
}

func (a *K8sPodsAdapter) Close() error {
	return nil
}

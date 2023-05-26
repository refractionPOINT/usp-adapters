package usp_k8s

import (
	"fmt"
	"testing"
	"time"

	"github.com/refractionPOINT/go-uspclient"
)

func TestEngine(t *testing.T) {
	k, err := NewK8sLogProcessor("/", uspclient.ClientOptions{
		DebugLog: func(msg string) {
			fmt.Println("DEBUG", msg)
		},
		OnWarning: func(msg string) {
			fmt.Println("WARN", msg)
		},
		OnError: func(err error) {
			fmt.Println("ERROR", err)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Second)

	k.Close()

	t.Error("done")
}

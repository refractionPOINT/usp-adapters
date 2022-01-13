package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type testElement struct {
	i int
	s string
}

func genTestElement(n int) []*testElement {
	batches := []*testElement{}
	for i := 0; i < n; i++ {
		batches = append(batches, &testElement{i: i})
	}
	return batches
}

func TestPipeliner(t *testing.T) {
	batches := genTestElement(20)
	nextBatch := 0
	var err error
	it := func() (Element, error) {
		if nextBatch >= len(batches) {
			return nil, nil
		}
		b := batches[nextBatch]
		nextBatch++
		return b, nil
	}

	stop := NewEvent()

	var close PipelinerClose
	it, close, err = Pipeliner(it, 5, func(e Element) Element {
		e.(*testElement).s = fmt.Sprintf("%d", e.(*testElement).i)
		return e
	})
	if err != nil {
		t.Errorf("Pipeliner: %v", err)
		return
	}

	for i := 0; i < 20; i++ {
		nextB, err := it()
		if err != nil {
			t.Errorf("it(): %v", err)
		}
		if nextB == nil {
			t.Errorf("expected a batch: %d", i)
		}
		expectedC := fmt.Sprintf("%d", i)
		if nextB.(*testElement).s != expectedC {
			t.Errorf("unexpected cursor: %s != %s", nextB.(*testElement).s, expectedC)
		}
	}

	nextB, err := it()
	if err != nil {
		t.Errorf("it(): %v", err)
	}
	if nextB != nil {
		t.Error("expected no more batches")
	}

	stop.Set()
	close()
}

func TestPipelinerClose(t *testing.T) {
	batches := genTestElement(20)
	nextBatch := 0
	var err error
	it := func() (Element, error) {
		if nextBatch >= len(batches) {
			return nil, nil
		}
		b := batches[nextBatch]
		nextBatch++
		return b, nil
	}

	var close PipelinerClose
	it, close, err = Pipeliner(it, 3, func(e Element) Element {
		e.(*testElement).s = fmt.Sprintf("%d", e.(*testElement).i)
		return e
	})
	if err != nil {
		t.Errorf("batchPrefetcher: %v", err)
		return
	}

	for i := 0; i < 5; i++ {
		nextB, err := it()
		if err != nil {
			t.Errorf("it(): %v", err)
		}
		if nextB == nil {
			t.Errorf("expected a batch: %d", i)
		}
		expectedC := fmt.Sprintf("%d", i)
		if nextB.(*testElement).s != expectedC {
			t.Errorf("unexpected cursor: %s != %s", nextB.(*testElement).s, expectedC)
		}
	}

	time.Sleep(5 * time.Second)

	close()
}

func TestPipelinerError(t *testing.T) {
	batches := genTestElement(20)
	nextBatch := 0
	var err error
	mIt := sync.Mutex{}
	it := func() (Element, error) {
		mIt.Lock()
		defer mIt.Unlock()
		if nextBatch >= 2 {
			return nil, errors.New("test-error")
		}
		b := batches[nextBatch]
		nextBatch++
		return b, nil
	}

	stop := NewEvent()

	var close PipelinerClose
	it, close, err = Pipeliner(it, 1, func(e Element) Element {
		e.(*testElement).s = fmt.Sprintf("%d", e.(*testElement).i)
		return e
	})
	if err != nil {
		t.Errorf("Pipeliner: %v", err)
		return
	}

	for i := 0; i < 20; i++ {
		nextB, err := it()
		if i == 2 {
			if err == nil {
				t.Error("should have errored")
			}
			break
		} else if err != nil {
			t.Errorf("it(%d): %v", i, err)
		}
		if nextB == nil {
			t.Errorf("expected a batch: %d", i)
		}
		expectedC := fmt.Sprintf("%d", i)
		if nextB.(*testElement).s != expectedC {
			t.Errorf("unexpected cursor: %s != %s", nextB.(*testElement).s, expectedC)
		}
	}

	stop.Set()
	close()
}

func TestPipelinerSize(t *testing.T) {
	it := func() (Element, error) {
		return nil, nil
	}

	_, _, err := Pipeliner(it, 0, func(e Element) Element {
		e.(*testElement).s = fmt.Sprintf("%d", e.(*testElement).i)
		return e
	})
	if err == nil {
		t.Error("invalid size should raise error")
		return
	}
}

func TestPipelinerSerial(t *testing.T) {
	batches := genTestElement(20)
	nextBatch := 0
	var err error
	it := func() (Element, error) {
		if nextBatch >= len(batches) {
			return nil, nil
		}
		b := batches[nextBatch]
		nextBatch++
		return b, nil
	}

	stop := NewEvent()

	var close PipelinerClose
	it, close, err = Pipeliner(it, 1, func(e Element) Element {
		e.(*testElement).s = fmt.Sprintf("%d", e.(*testElement).i)
		return e
	})
	if err != nil {
		t.Errorf("Pipeliner: %v", err)
		return
	}

	for i := 0; i < 20; i++ {
		nextB, err := it()
		if err != nil {
			t.Errorf("it(): %v", err)
		}
		if nextB == nil {
			t.Errorf("expected a batch: %d", i)
		}
		expectedC := fmt.Sprintf("%d", i)
		if nextB.(*testElement).s != expectedC {
			t.Errorf("unexpected cursor: %s != %s", nextB.(*testElement).s, expectedC)
		}
	}

	nextB, err := it()
	if err != nil {
		t.Errorf("it(): %v", err)
	}
	if nextB != nil {
		t.Error("expected no more batches")
	}

	stop.Set()
	close()
}

func BenchmarkPipeliner1(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runPipelinerBench(1, 1000)
	}
}
func BenchmarkPipeliner2(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runPipelinerBench(2, 1000)
	}
}
func BenchmarkPipeliner4(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runPipelinerBench(4, 1000)
	}
}
func BenchmarkPipeliner8(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runPipelinerBench(8, 1000)
	}
}
func BenchmarkPipeliner16(b *testing.B) {
	for n := 0; n < b.N; n++ {
		runPipelinerBench(16, 1000)
	}
}

func runPipelinerBench(nParallel int, nElem int) {
	nextElem := 0
	var err error
	testData := []byte(`{"type":"endpoint.event.netconn","process_guid":"XXXXXXX-0237b20a-00000239-00000000-1d7baef8fce21c0","parent_guid":"XXXXXXX-0237b20a-00000213-00000000-1d7baef8ec82370","backend_timestamp":"2021-10-11 19:46:43 +0000 UTC","org_key":"XXXXXX","device_id":"37204490","device_name":"xxxxx","device_external_ip":"66.66.66.66","device_os":"MAC","device_group":"","action":"ACTION_CONNECTION_CREATE | ACTION_CONNECTION_ESTABLISHED","schema":1,"device_timestamp":"2021-10-08 23:31:58.739 +0000 UTC","process_terminated":false,"process_reputation":"REP_RESOLVING","parent_reputation":"REP_RESOLVING","process_pid":569,"parent_pid":531,"process_publisher":[{"name":"Developer ID Application: Google, Inc. (EQHXZ8M8AV)","state":"FILE_SIGNATURE_STATE_SIGNED | FILE_SIGNATURE_STATE_VERIFIED"}],"process_path":"/Applications/Google Chrome.app/Contents/Frameworks/Google Chrome Framework.framework/Versions/94.0.4606.71/Helpers/Google Chrome Helper.app/Contents/MacOS/Google Chrome Helper","parent_path":"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome","process_hash":["","d0a26ba7e0fa55cc5a861277da848b46493492dc22e2ae1ddbfbb01e5bb143fe"],"parent_hash":["","2d21a51d33385fe918747904a095578a5d7750cabb842097b58ee9c6289fbab0"],"process_cmdline":"Google Chrome Helper --type=utility --utility-sub-type=network.mojom.NetworkService --field-trial-handle=1718379636,16741980054890516258,7839217863320355178,131072 --lang=en-US --service-sandbox-type=network --shared-files --seatbelt-client=27","parent_cmdline":"Google Chrome","process_username":"xxxxxx","sensor_action":"ACTION_ALLOW","event_origin":"EDR","remote_port":443,"remote_ip":"66.66.66.66","local_port":55978,"local_ip":"66.66.66.66","netconn_domain":"xxxx.xxx.com","netconn_inbound":false,"netconn_protocol":"PROTO_TCP"}`)
	it := func() (Element, error) {
		if nextElem >= nElem {
			return nil, nil
		}
		nextElem++
		return testData, nil
	}

	stop := NewEvent()

	var close PipelinerClose
	it, close, err = Pipeliner(it, nParallel, func(e Element) Element {
		tmp := Dict{}
		json.Unmarshal(e.([]byte), &tmp)
		return tmp
	})
	if err != nil {
		panic(fmt.Sprintf("Pipeliner: %v", err))
	}

	for i := 0; i < nElem; i++ {
		_, err := it()
		if err != nil {
			panic(fmt.Sprintf("it(): %v", err))
		}
	}

	_, err = it()
	if err != nil {
		panic(fmt.Sprintf("it(): %v", err))
	}

	stop.Set()
	close()
}

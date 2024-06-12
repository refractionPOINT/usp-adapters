package utils

import (
	"testing"
	"time"
)

func TestLocalDeduper_CheckAndAdd(t *testing.T) {
	window := 100 * time.Millisecond
	ttl := 500 * time.Millisecond

	d, err := NewLocalDeduper(window, ttl)
	if err != nil {
		t.Fatalf("error creating deduper: %v", err)
	}

	if d.CheckAndAdd("key1") {
		t.Error("key1 should not exist")
	}
	if !d.CheckAndAdd("key1") {
		t.Error("key1 should exist")
	}
	if d.CheckAndAdd("key2") {
		t.Error("key2 should not exist")
	}
	if !d.CheckAndAdd("key2") {
		t.Error("key1 should exist")
	}
	time.Sleep(ttl+time.Second)
	if d.CheckAndAdd("key1") {
		t.Error("key1 should not exist after ttl")
	}
	if d.CheckAndAdd("key2") {
		t.Error("key1 should exist after ttl")
	}
	if d.CheckAndAdd("key3") {
		t.Error("key3 should not exist")
	}

	d.Close()
}
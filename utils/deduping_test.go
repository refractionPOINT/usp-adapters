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
	time.Sleep(ttl + time.Second)
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

func TestLocalDeduper_CheckDoesNotRecord(t *testing.T) {
	d, err := NewLocalDeduper(time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatalf("error creating deduper: %v", err)
	}
	defer d.Close()

	cd, ok := d.(CheckingDeduper)
	if !ok {
		t.Fatal("the local deduper must implement CheckingDeduper")
	}
	if cd.Check("key1") {
		t.Error("key1 should not exist")
	}
	if cd.Check("key1") {
		t.Error("Check must not record key1")
	}
	if cd.CheckAndAdd("key1") {
		t.Error("key1 should not exist before it is added")
	}
	if !cd.Check("key1") {
		t.Error("key1 should exist once added")
	}
}

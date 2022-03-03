// +build windows

package usp_wel

// Shim code for the Windows API
// to subscribe to real-time WEL.

import (
	"syscall"
	"unsafe"
)

const (
	TRUE  = int(1)
	FALSE = int(0)
	NULL  = uintptr(0)

	EvtRenderEventXml          = 1
	EvtSubscribeToFutureEvents = 1
	EvtSubscribeActionError    = 0
	EvtSubscribeActionDeliver  = 1
)

type EVT_HANDLE = uintptr
type HANDLE = uintptr
type BOOL = int
type PVOID = uintptr
type DWORD = uint32

type EVT_SUBSCRIBE_NOTIFY_ACTION = int
type EVT_SUBSCRIBE_CALLBACK func(Action EVT_SUBSCRIBE_NOTIFY_ACTION, UserContext PVOID, Event EVT_HANDLE) uintptr

var (
	welAPI     = syscall.NewLazyDLL("wevtapi.dll")
	fClose     = welAPI.NewProc("EvtClose")
	fSubscribe = welAPI.NewProc("EvtSubscribe")
	fRender    = welAPI.NewProc("EvtRender")
)

func EvtClose(Object EVT_HANDLE) error {
	r1, _, err := fClose.Call(Object)
	if BOOL(r1) == FALSE {
		return err
	}
	return nil
}

func EvtSubscribe(
	Session EVT_HANDLE,
	SignalEvent HANDLE,
	ChannelPath string,
	Query string,
	Bookmark EVT_HANDLE,
	context PVOID,
	Callback EVT_SUBSCRIBE_CALLBACK,
	Flags DWORD) (EVT_HANDLE, error) {
	channelPath, err := syscall.UTF16PtrFromString(ChannelPath)
	if err != nil {
		return EVT_HANDLE(0), err
	}
	query, err := syscall.UTF16PtrFromString(Query)
	if err != nil {
		return EVT_HANDLE(0), err
	}
	hSub, _, err := fSubscribe.Call(
		uintptr(Session),
		uintptr(SignalEvent),
		uintptr(unsafe.Pointer(channelPath)),
		uintptr(unsafe.Pointer(query)),
		uintptr(Bookmark),
		uintptr(context),
		syscall.NewCallback(Callback),
		uintptr(Flags))
	if hSub == NULL {
		return EVT_HANDLE(hSub), err
	}
	return EVT_HANDLE(hSub), nil
}

func EvtRenderXML(Context EVT_HANDLE) ([]byte, error) {
	// 65536 buffsize
	const bufferSize = 1024 * 128
	var buffer [bufferSize]byte
	var BufferUsed DWORD
	var PropertyCount DWORD

	isSuccess, _, err := fRender.Call(
		uintptr(0),
		uintptr(Context),
		uintptr(EvtRenderEventXml),
		uintptr(bufferSize),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(unsafe.Pointer(&BufferUsed)),
		uintptr(unsafe.Pointer(&PropertyCount)))

	if BOOL(isSuccess) == FALSE {
		return nil, err
	}
	return buffer[:BufferUsed], nil
}

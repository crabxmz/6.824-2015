package pbservice

import "viewservice"

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongServer     = "ErrWrongServer"
	NotPrimary         = "NotPrimary"
	ForwardFailed      = "ForwardFailed"
	DuplicateRequest   = "DuplicateRequest"
	InternalFailed     = "InternalFailed"
	ViewserviceUnavail = "ViewserviceUnavail"
)
const (
	PRIMARY   = 1
	BACKUP    = 2
	VOLUNTEER = 3
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Op      uint //0:put, 1:append
	Forward bool //true:from client, false:redirect between c/s
	Seq     string
	Viewnum uint
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Sync    bool
	Viewnum uint
}

type GetReply struct {
	Err   Err
	Value string
}

// Your RPC definitions here.
type GetViewArgs struct {
}
type GetViewReply struct {
	view viewservice.View
	Err  Err
}

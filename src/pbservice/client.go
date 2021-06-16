package pbservice

import (
	"crypto"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/big"
	"net/rpc"
	"reflect"
	"time"
	"viewservice"
)

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	cur_view viewservice.View
	// retry_num uint
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
func Hash(objs ...interface{}) []byte {
	digester := crypto.MD5.New()
	for _, ob := range objs {
		fmt.Fprint(digester, reflect.TypeOf(ob))
		fmt.Fprint(digester, ob)
	}
	return digester.Sum(nil)
}
func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	log.Println("New clerk init, view service:", vshost)
	ck.UpdateView()
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{}
	args.Key = key
	args.Sync = false
	args.Viewnum = ck.cur_view.Viewnum
	ret := ""
	for {
		rsp := &GetReply{}
		ok := call(ck.cur_view.Primary, "PBServer.Get", args, rsp)
		if !ok {
			// log.Println("Client call rpc PBServer.Get() failed ...")
		} else if rsp.Err != "" {
			log.Println("rpc Get return error,", rsp.Err, "Primary:", ck.cur_view.Primary)
		} else {
			ret = rsp.Value
			break
		}
		if ck.UpdateView() != nil { //check if primary has changed
			return ""
		}
		time.Sleep(viewservice.PingInterval)
	}
	return ret
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op string) {

	// Your code here.
	args := &PutAppendArgs{}
	args.Forward = true
	args.Key = key
	args.Value = value
	args.Viewnum = ck.cur_view.Viewnum
	// args.Seq = hex.EncodeToString(Hash(key, value, op, time.Now()))
	args.Seq = hex.EncodeToString(Hash(key, value, op))
	switch op {
	case "Put":
		args.Op = 0
	case "Append":
		args.Op = 1
	}
	for {
		// log.Println(ck.cur_view.Primary, ck.cur_view.Backup)
		rsp := &PutAppendReply{}
		ok := call(ck.cur_view.Primary, "PBServer.PutAppend", args, rsp)
		if !ok {
			log.Printf("Client PutAppend rpc to %s failed, Key:%s, Value:%s\n", ck.cur_view.Primary, key, value)
		} else if rsp.Err != "" {
			log.Printf("Client PutAppend rpc to %s error, Key:%s, Value:%s\n", ck.cur_view.Primary, key, value)
			log.Println(rsp.Err, ck.cur_view.Primary)
			if rsp.Err == ForwardFailed {
				ck.UpdateView()
				if ck.cur_view.Backup == "" {
					break
				}
			} else if rsp.Err == DuplicateRequest {
				break
			}
		} else {
			// log.Printf("Client PutAppend rpc to %s done! Key:%s, Value:%s\n", ck.cur_view.Primary, key, value)
			break
		}
		if ck.UpdateView() != nil { //check if primary has changed
			return
		}
		time.Sleep(viewservice.PingInterval)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) UpdateView() error {
	view_, ok := ck.vs.Get()
	if !ok {
		log.Println("Clerk: viewservice died ...")
		return errors.New("Viewservice Failed")
	} else {
		if !reflect.DeepEqual(view_, ck.cur_view) {
			log.Printf("client update view, now p is |%s|,b is |%s|\n", view_.Primary, view_.Backup)
			ck.cur_view = view_
		}
	}
	return nil
}

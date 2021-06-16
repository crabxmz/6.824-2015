package pbservice

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"viewservice"
)

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	kvmap      map[string]string
	req        map[string]bool
	cur_view   viewservice.View
	sync_retry uint
	identity   uint
	vs_avail   int32
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	if atomic.LoadInt32(&pb.vs_avail) == 0 { //view service unreachable ...
		return pb.handleVSFailure(args, reply)
	}
	reply.Err = ""
	if args.Viewnum > pb.cur_view.Viewnum {
		pb.mu.Lock()
		e := pb.updateView()
		pb.mu.Unlock()
		if e != nil {
			log.Println(e)
			return nil
		}
	}
	if pb.me != pb.cur_view.Primary {
		// log.Println(pb.me, pb.cur_view.Primary)
		reply.Err = ErrWrongServer
		return nil
	}
	if args.Sync == true {
		switch args.Key {
		case "SyncKV":
			buf, e := json.Marshal(pb.kvmap)
			if e == nil {
				reply.Value = base64.StdEncoding.EncodeToString(buf)
			} else {
				reply.Err = "Encode error"
			}
		case "SyncReq":
			buf, e := json.Marshal(pb.req)
			if e == nil {
				reply.Value = base64.StdEncoding.EncodeToString(buf)
			} else {
				reply.Err = "Encode error"
			}
		default:
			reply.Err = "Unknown Sync Op ..."
		}
	} else {
		pb.mu.Lock()
		defer pb.mu.Unlock()
		k := args.Key
		if val, ok := pb.kvmap[k]; ok {
			reply.Value = val
		} else {
			reply.Value = ""
		}
	}
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	if atomic.LoadInt32(&pb.vs_avail) == 0 { //view service unreachable ...
		return errors.New(ViewserviceUnavail)
	}
	//check view update
	pb.mu.Lock()
	defer pb.mu.Unlock()
	var e Err
	e = ""
	// requests to old primary redirect to new primary
	if pb.cur_view.Backup == pb.me { // is backup
		if args.Forward == true { //from client
			// e = pb.redirect(pb.cur_view.Primary, args)
			// if e == "" { //redirect to primary, backup commit
			// 	e = pb.commitKV(args)
			// }
			e = ErrWrongServer
		} else { //from primary
			e = pb.commitKV(args)
		}
	} else if pb.cur_view.Primary == pb.me { // is primary
		if args.Forward == true { //from client
			if pb.cur_view.Backup != "" { //backup exist
				e = pb.redirect(pb.cur_view.Backup, args)
			}
			if e == "" || e == DuplicateRequest { //redirect to backup ok, primary commit
				// log.Println("redirect ok")
				e = pb.commitKV(args)
			}
		} else { //from backup
			e = pb.commitKV(args)
		}
	} else {
		log.Println(pb.me, "fuck, why do i get this?")
		e = ErrWrongServer
	}
	reply.Err = e
	// log.Printf("i am %s, p is %s, b is %s, k=%s, v=%s\n", pb.me, pb.cur_view.Primary, pb.cur_view.Backup, args.Key, pb.kvmap[args.Key])
	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// log.Println(pb.me)
	e := pb.updateView()
	if e == nil {
		v := &pb.cur_view
		pb.vs.Ping(v.Viewnum) //suppose view service never fail
		switch pb.me {
		case v.Primary:
			pb.identity = PRIMARY
		case v.Backup:
			pb.identity = BACKUP
		default: //volunteer
			pb.identity = VOLUNTEER
		}
	} else {
		// log.Println(e)
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	//kvmap
	pb.kvmap = make(map[string]string)
	//req_map
	pb.req = make(map[string]bool)
	//sync retry num
	pb.sync_retry = 5
	//register to view serive
	v, e := pb.vs.Ping(0)
	if e == nil {
		ok := true
		pb.vs_avail = 1
		for ; ok; v, ok = pb.vs.Get() {
			if v.Primary == me {
				pb.identity = PRIMARY
				pb.cur_view = v
				pb.vs.Ping(v.Viewnum)
				break
			}
			if v.Backup == me {
				pb.identity = BACKUP
				pb.cur_view = v
				if !pb.syncKVFromPrimary(v.Primary) {
					log.Println("syncKVFromPrimary error, Primary is", v.Primary, ", i am", pb.me)
				}
				if !pb.syncReqFromPrimary(v.Primary) {
					log.Println("syncReqFromPrimary error, Primary is", v.Primary, ", i am", pb.me)
				}
				break
			}
			if v.Backup != "" && v.Primary != "" {
				pb.identity = VOLUNTEER
				pb.cur_view = v
				break
			}
		}
		if !ok {
			log.Println("viewservice died ...", e)
			pb.vs_avail = 0
		}
	} else {
		log.Println("ping(0) error, viewservice died ...", e)
		pb.vs_avail = 0
	}
	log.Printf("new server start, p is %s and b is %s\n", v.Primary, v.Backup)
	//pb.* initializations over
	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}

func (pb *PBServer) syncKVFromPrimary(pri string) bool {
	args := &GetArgs{"SyncKV", true, pb.cur_view.Viewnum}
	rsp := &GetReply{}
	if pri == pb.me {
		log.Fatalln("Deadlock")
	}
	ok := pb.callretry(pb.sync_retry, pri, "PBServer.Get", args, rsp)
	if !ok || rsp.Err != "" {
		log.Println("syncKVFromPrimary rpc failed, Err:", rsp.Err, ", i am:", pb.me)
		return false
	}
	decodeBytes, err := base64.StdEncoding.DecodeString(rsp.Value)
	if err != nil {
		log.Println(err)
		return false
	}
	e := json.Unmarshal(decodeBytes, &pb.kvmap)
	if e != nil {
		log.Println(e)
		return false
	}
	return true
}

func (pb *PBServer) syncReqFromPrimary(pri string) bool {
	args := &GetArgs{"SyncReq", true, pb.cur_view.Viewnum}
	rsp := &GetReply{}
	if pri == pb.me {
		log.Fatalln("Deadlock")
	}
	ok := pb.callretry(pb.sync_retry, pri, "PBServer.Get", args, rsp)
	if !ok || rsp.Err != "" {
		log.Println("syncReqFromPrimary rpc failed, Err:", rsp.Err, ", i am:", pb.me)
		return false
	}
	decodeBytes, err := base64.StdEncoding.DecodeString(rsp.Value)
	if err != nil {
		log.Println(err)
		return false
	}
	e := json.Unmarshal(decodeBytes, &pb.req)
	if e != nil {
		log.Println(e)
		return false
	}
	return true
}
func (pb *PBServer) callretry(ntime uint, srv string, rpcname string, args interface{}, reply interface{}) bool {
	for i := uint(0); i < ntime; i++ {
		ok := call(srv, rpcname, args, reply)
		if ok {
			return true
		}
		// time.Sleep(viewservice.PingInterval)
	}
	return false
}
func (pb *PBServer) updateView() error {
	view_, ok := pb.vs.Get()
	if !ok {
		atomic.StoreInt32(&pb.vs_avail, 0)
		// log.Println("viewservice died ...")
		return errors.New(ViewserviceUnavail)
	} else {
		atomic.StoreInt32(&pb.vs_avail, 1)
	}
	if view_.Viewnum != pb.cur_view.Viewnum {
		if view_.Backup == pb.me && pb.cur_view.Backup != pb.me { //i become backup
			// if view_.Primary == pb.me {
			// 	return errors.New("deadlock")
			// }
			pb.cur_view = view_
			if !pb.syncKVFromPrimary(view_.Primary) {
				return errors.New("syncKVFromPrimary error")
			}
			if !pb.syncReqFromPrimary(view_.Primary) {
				return errors.New("syncReqFromPrimary error")
			}
		} else if view_.Primary == pb.me && pb.cur_view.Primary != pb.me { //i become primary
			v, ok := pb.vs.Get()
			if !ok {
				atomic.StoreInt32(&pb.vs_avail, 0)
				return errors.New(ViewserviceUnavail)
			}
			pb.cur_view = v
			if _, e := pb.vs.Ping(v.Viewnum); e != nil {
				atomic.StoreInt32(&pb.vs_avail, 0)
				return errors.New(ViewserviceUnavail)
			}
		} else {
			pb.cur_view = view_
		}
		log.Printf("server %s update view, now p is |%s|,b is |%s|\n", pb.me, view_.Primary, view_.Backup)
	}
	return nil
}
func (pb *PBServer) checkDup(args *PutAppendArgs) Err {
	_, ok := pb.req[args.Seq]
	if ok {
		// log.Println(pb.me, "Dup", args.Key, pb.kvmap[args.Key])
		return DuplicateRequest
	}
	return ""
}

func (pb *PBServer) commitKV(args *PutAppendArgs) Err {
	if e := pb.checkDup(args); e != "" {
		return e
	}
	switch args.Op {
	case 0:
		pb.kvmap[args.Key] = args.Value
	case 1:
		if val, ok := pb.kvmap[args.Key]; ok {
			pb.kvmap[args.Key] = val + args.Value
		} else {
			pb.kvmap[args.Key] = args.Value
		}
	}
	// log.Println(pb.me, "commit", args.Key, args.Value)
	pb.req[args.Seq] = true
	return ""
}
func (pb *PBServer) redirect(target string, args *PutAppendArgs) Err {
	frdArgs := *args
	frdArgs.Viewnum = pb.cur_view.Viewnum
	frdArgs.Forward = false
	frdRsp := &PutAppendReply{}
	if target == pb.me {
		log.Fatalln("Deadlock")
	}
	_f := pb.callretry(pb.sync_retry, target, "PBServer.PutAppend", &frdArgs, frdRsp)
	if !_f {
		return ForwardFailed
	}
	return frdRsp.Err
}

func (pb *PBServer) handleVSFailure(args *GetArgs, reply *GetReply) error {
	if args.Sync == true { //if from other pb server ,do noting
		reply.Err = ViewserviceUnavail
		log.Println(reply.Err)
		return nil
	}
	another := ""
	if pb.cur_view.Primary != "" { //if no cache ,do noting
		another = pb.cur_view.Primary
	} else if pb.cur_view.Backup != "" {
		another = pb.cur_view.Backup
	} else {
		reply.Err = ViewserviceUnavail
		log.Println(reply.Err)
		return nil
	}
	x := &GetViewArgs{}
	y := &GetViewReply{}
	ok := call(another, "PBServer.GetView", x, y)
	if !ok { //if get view failed ,return
		reply.Err = ViewserviceUnavail
		log.Println(reply.Err)
		return nil
	}
	pb.cur_view = y.view
	ok = call(pb.cur_view.Primary, "PBServer.Get", args, reply)
	if !ok {
		reply.Err = ViewserviceUnavail
		log.Println(reply.Err)
		return nil
	}
	return nil
}

func (pb *PBServer) GetView(args *GetViewArgs, reply *GetViewReply) error {
	reply.view = pb.cur_view
	reply.Err = ""
	return nil
}

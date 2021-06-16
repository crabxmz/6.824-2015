package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	views     []View //0:commited 1:tmp
	ttl       []uint //0:primary 1:backup
	acked     uint
	volunteer map[string]uint
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	if args.Me == "" {
		log.Println("ping empty clerk, do nothing ...")
		return nil
	}
	cv := &vs.views[0]
	tmp := &vs.views[1]
	safe_assign_pri := func(new_pri string) {
		if vs.acked == cv.Viewnum {
			cv.Primary = new_pri
			cv.Viewnum += 1
			*tmp = *cv
			log.Println("Primary inited:", new_pri)
		} else {
			tmp.Primary = new_pri
			tmp.Viewnum = cv.Viewnum + 1
			log.Printf("wait for ack %d from primary when assign pri\n", cv.Viewnum)
		}
	}
	safe_assign_bk := func(new_bk string) {
		if vs.acked == cv.Viewnum {
			cv.Backup = new_bk
			cv.Viewnum += 1
			*tmp = *cv
			log.Println("Backup inited:", new_bk)
		} else {
			tmp.Backup = new_bk
			tmp.Viewnum = cv.Viewnum + 1
			log.Printf("wait for ack %d from primary when assign backup\n", cv.Viewnum)
		}
	}
	if args.Viewnum == 0 {
		if cv.Primary == "" {
			safe_assign_pri(args.Me)
		} else if cv.Backup == "" && cv.Primary != args.Me {
			safe_assign_bk(args.Me)
		} else if cv.Backup == args.Me {
			log.Println("Backup rebooted", args.Me)
		} else if cv.Primary == args.Me {
			log.Println("Primary rebooted", args.Me)
		} else {
			log.Println("ping from 3rd client:", args.Me)
			if _, ok := vs.volunteer[args.Me]; !ok {
				log.Println("New volunteer:", args.Me)
			}
			vs.volunteer[args.Me] = DeadPings
		}
	} else {
		if args.Me == cv.Primary {
			vs.ttl[0] = DeadPings
			if args.Viewnum <= cv.Viewnum {
				if args.Viewnum > vs.acked {
					vs.acked = args.Viewnum
				}
				if vs.acked == cv.Viewnum {
					if !reflect.DeepEqual(*cv, *tmp) {
						*cv = *tmp
					}
				}
			} else {
				log.Printf("err!ping(%d) > viewnum %d\n", args.Viewnum, cv.Viewnum)
			}
		} else if args.Me == cv.Backup {
			vs.ttl[1] = DeadPings
		} else {
			vs.volunteer[args.Me] = DeadPings
		}
	}
	reply.View = *cv

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	reply.View = vs.views[0]
	// log.Println("call from client", vs.views[0])
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()
	cv := &vs.views[0]
	tmp := &vs.views[1]
	safe_assign_pri := func() error {
		if vs.acked == cv.Viewnum {
			cv.Primary = cv.Backup
			cv.Backup = ""
			cv.Viewnum += 1
			*tmp = *cv
		} else {
			log.Printf("wait for ack %d from primary when primary timeout\n", cv.Viewnum)
		}
		return nil
	}
	safe_assign_bk := func() error {
		if vs.acked == cv.Viewnum {
			if len(vs.volunteer) == 0 {
				cv.Backup = ""
				log.Println("!@(*#&!@($no volunteer")
			} else {
				for k, v := range vs.volunteer {
					cv.Backup = k
					vs.ttl[1] = v
					log.Println("!@(*#&!@($volunteer->backup")
					delete(vs.volunteer, k)
					break
				}
			}
			cv.Viewnum += 1
			*tmp = *cv
		} else {
			log.Printf("wait for ack %d from backup when backup timeout\n", cv.Viewnum)
		}
		return nil
	}
	if vs.ttl[0] == 0 && cv.Primary != "" {
		log.Printf("Primary timeout on view %d, acked view %d\n", cv.Viewnum, vs.acked)
		safe_assign_pri()
	}
	if vs.ttl[1] == 0 && cv.Backup != "" {
		log.Printf("Backup timeout on view %d, acked view %d\n", cv.Viewnum, vs.acked)
		safe_assign_bk()
	}
	vs.ttl[0] -= 1
	vs.ttl[1] -= 1
	for k := range vs.volunteer {
		if vs.volunteer[k] == 0 {
			delete(vs.volunteer, k)
			log.Printf("volunteer server %s timeout, remove it\n", k)
		} else {
			vs.volunteer[k] -= 1
		}
	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
	log.Println("View service closed ...")
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.views = make([]View, 2)
	vs.ttl = make([]uint, 2)
	vs.volunteer = make(map[string]uint)
	vs.ttl[0] = DeadPings
	vs.ttl[1] = DeadPings
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}

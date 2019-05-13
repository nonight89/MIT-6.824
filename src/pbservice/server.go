package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	curView    viewservice.View
	data       map[string]string
	dup        map[int64]Err
}

func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	if pb.curView.Backup == pb.me {
		pb.data[args.Key] = args.Value
		pb.dup[args.Xid] = OK
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) GetReplica(args *GetReplicaArgs, reply *GetReplicaReply) error {
	if pb.curView.Primary == pb.me {
		reply.Data = pb.data
		reply.Dup = pb.dup
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	if pb.curView.Primary == pb.me {
		value, ok := pb.data[args.Key]
		if ok {
			reply.Err = OK
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
			reply.Value = ""
		}
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	// Your code here.
	if pb.curView.Primary == pb.me {
		dupValue, ok := pb.dup[args.Xid]
		if ok == false {
			// 1. get the final value of args.Key
			var val string
			if args.Op == "Put" {
				val = args.Value
			} else if args.Op == "Append"{
				value, ok := pb.data[args.Key]
					val = args.Value
				if ok {
					val = value + args.Value
				} else {
					val = args.Value
				}
			}
			// 2. forward final key/value to backup if backup exists
			if pb.curView.Backup != "" {
				forwardArgs := &ForwardArgs{}
				forwardArgs.Key = args.Key
				forwardArgs.Value = val
				forwardArgs.Xid = args.Xid
				var forwardReply ForwardReply

				ok := call(pb.curView.Backup, "PBServer.Forward", forwardArgs, &forwardReply)
			
				// 3. store key/value on primary and insert xid to dup
				if ok && forwardReply.Err == OK {
					pb.data[args.Key] = val
					pb.dup[args.Xid] = OK
					reply.Err = pb.dup[args.Xid]
					log.Printf("[Server:%s]PutAppend with backup {%s:%s}", pb.me, args.Key, val)
				} else {
					reply.Err = ErrReplica
				}
			} else {
				pb.data[args.Key] = val
				pb.dup[args.Xid] = OK
				reply.Err = pb.dup[args.Xid]
				log.Printf("[Server:%s]PutAppend {%s:%s}", pb.me, args.Key, val)
			}
		} else {
			reply.Err = dupValue
		}
	} else {
		reply.Err = ErrWrongServer
	}
	pb.mu.Unlock()
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
	view, _ := pb.vs.Ping(pb.curView.Viewnum)
	//pb.mu.Lock()
	if view.Viewnum != pb.curView.Viewnum {
		if pb.me == view.Backup {
			//TODO full replication from primary
			getReplicaArgs := &GetReplicaArgs{}
			getReplicaArgs.Srv = pb.me
			var getReplicaReply GetReplicaReply

			ok := call(view.Primary, "PBServer.GetReplica", getReplicaArgs, &getReplicaReply)

			if ok == true && getReplicaReply.Err == OK{
				log.Printf("[Server:%s]tick copy primary data completely", pb.me)
				pb.mu.Lock()
				pb.data = getReplicaReply.Data
				pb.dup = getReplicaReply.Dup
				pb.mu.Unlock()
				pb.curView = view
			} else {
				log.Printf("[Server:%s]GetReplica Failure", pb.me)
			}
		} else {
			pb.curView = view
		}
	}
	//pb.mu.Unlock()
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
	pb.curView = viewservice.View{0, "", ""}
	pb.data = make(map[string]string)
	pb.dup = make(map[int64]Err)

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

package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string


	// Your declarations here.
	primaryAck uint
	pingTrack  map[string]time.Time
	view       View
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	vs.mu.Lock()
	log.Printf("[viewserver]%+v PING, vs state: vs.view=%+v, vs.primaryAck=%d", args, vs.view, vs.primaryAck)
	// Your code here.

	//0. keep track by setting pingMap
	// if primary viewnum lag behind more than 1 with viewserver, we consider the priamry is dead
	if !(vs.view.Primary == args.Me && vs.view.Viewnum > args.Viewnum + 1) {
		vs.pingTrack[args.Me] = time.Now()
	}
	
	if vs.view.Primary == "" && vs.view.Backup == "" {
		// Should prevent uninitialized server to be primary
		// When primary crashes while there is no backup, we must reboot primary to recovery the service
		// When primary crashes while backup crashes simultaneously, we can recovery the service through both primary and backup 
		if vs.view.Viewnum == 0 && vs.view.Viewnum == vs.primaryAck {
			vs.view.Primary = args.Me
			vs.view.Viewnum += 1
			log.Printf("[viewserver]add primary %s, reply=%+v", args.Me, vs.view)
		}
	} else if vs.view.Primary != "" && vs.view.Backup == "" {
		if vs.view.Primary == args.Me {
			//primary acknowledge
			if vs.primaryAck == args.Viewnum - 1 {
				vs.primaryAck = args.Viewnum
				log.Printf("[viewserver]Primary %s ack %d",args.Me, vs.primaryAck)
			}
		} else if vs.view.Viewnum == vs.primaryAck {
			//Add Backup 
			vs.view.Backup = args.Me
			vs.view.Viewnum += 1
		}
	} else if vs.view.Primary != "" && vs.view.Backup != "" {
		if vs.view.Primary == args.Me {
			//primary acknowledge
			if vs.primaryAck == args.Viewnum - 1 {
				vs.primaryAck = args.Viewnum
				log.Printf("[viewserver]Primary %s ack %d",args.Me, vs.primaryAck)
			}
		}
	}
	reply.View = vs.view
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	reply.View = vs.view
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
	if vs.view.Viewnum == vs.primaryAck {
		currentTime := time.Now()
		// primary liveness check
		if vs.view.Primary != "" {
			primaryLastPingTime := vs.pingTrack[vs.view.Primary]
			//log.Printf("[viewserver]tick, primary last ping: %+v", primaryLastPingTime)
			if primaryLastPingTime.Add(PingInterval * DeadPings).Before(currentTime) {
				vs.view.Primary = ""
			}
		}

		// backup liveness check
		if vs.view.Backup != "" {
			backupLastPingTime := vs.pingTrack[vs.view.Backup]
			//log.Printf("[viewserver]tick, backup last ping: %+v", backupLastPingTime)
			if backupLastPingTime.Add(PingInterval * DeadPings).Before(currentTime) {
				vs.view.Backup = ""
			}
		}

		if vs.view.Primary == "" && vs.view.Backup != "" {
			//promote backup to primary
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = ""
			vs.view.Viewnum += 1
			log.Printf("[viewserver]tick, promote backup to primary, primary: %s", vs.view.Primary)
		}
	}
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
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
	vs.pingTrack = make(map[string]time.Time)
	vs.primaryAck = 0
	vs.view = View{0, "", ""}

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

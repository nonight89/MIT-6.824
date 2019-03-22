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

	log.Printf("[viewserver]receive a ping: %+v", args)
	// Your code here.
	//0. keep track by setting pingMap
	vs.pingTrack[args.Me] = time.Now()

	//1. primary acknowledge
	if args.Me == vs.view.Primary {
		if vs.primaryAck != args.Viewnum {
			log.Printf("[viewserver]this is primary ack=%d", args.Viewnum)
			vs.primaryAck = args.Viewnum
		}

		if args.Viewnum == 0 {
			//primary restarted
			temp := vs.view.Primary
			vs.view.Primary = vs.view.Backup
			vs.view.Backup = temp 
			vs.view.Viewnum += 1
			log.Printf("[viewserver]get primary Ping(0), promote backup to primary, primary: %s", vs.view.Primary)
		}

		reply.View = vs.view
		return nil
	}

	//2. view switch
	// get first ping
	if vs.view.Primary == "" && vs.view.Backup == "" {
		vs.view.Primary = args.Me
		vs.view.Viewnum += 1
		reply.View = vs.view
		log.Printf("[viewserver]primary initialization, view: %+v", vs.view)
		return nil
	}

	if vs.view.Viewnum == vs.primaryAck {
		// get first backup ping
		if vs.view.Primary != "" && vs.view.Backup == "" {
			vs.view.Backup = args.Me
			vs.view.Viewnum += 1
			reply.View = vs.view	
			log.Printf("[viewserver]backup initialization, view: %+v", vs.view)
			return nil
		}

		// client restarted
	}

	reply.View = vs.view
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

		if vs.view.Primary == "" && vs.view.Backup == "" {
			vs.view.Viewnum += 1
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
	vs.primaryAck = 1024
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

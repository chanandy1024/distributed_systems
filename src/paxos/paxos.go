package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

const (
	PrepareOK  = "PREPARE-OK"
	AcceptOK   = "ACCEPT-OK"
	DecideOK   = "DECIDE-OK"
	PrepareErr = "PREPARE-ERR"
	AcceptErr  = "ACCEPT-ERR"
)

// RPCs
type PrepareArgs struct {
	Seq int
	N   int
}

type PrepareReply struct {
	N_a     int
	V_a     interface{}
	DoneSeq int    // done sequence number
	Status  string // whether its PROPOSE-OK or ERR
}

type AcceptArgs struct {
	Seq int
	N   int
	V   interface{}
}

type AcceptReply struct {
	DoneSeq int    // done sequence number
	Status  string // whether its ACCEPT-OK or ERR
}

type DecideArgs struct {
	Seq int
	N   int
	V   interface{}
}

type DecideReply struct {
	DoneSeq int    // done sequence number
	Status  string // whether its DECIDE-OK or ERR
}

type State struct {
	Np   int         // highest proposal number seen to date
	Na   int         // highest accepted proposal
	Va   interface{} // value of highest accepted proposal
	Done bool        // whether consensus has been reached
  // Value interface{} // decided value
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int           // index into peers[]
	instances  map[int]State // map of instances, from seq to state
	done       map[int]int   // map of sequence numbers, used in Min(), from peer index to sequence number
	maxSeqNum  int           // maximum sequence number
}

func getMax(n1 int, n2 int) int {
	if n1 > n2 {
		return n1
	}
	return n2
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func (px *Paxos) PrepAcceptor(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	_, exist := px.instances[args.Seq]
	if !exist {
		px.instances[args.Seq] = State{-1, -1, nil, false}
	}
	instance := px.instances[args.Seq]
	// update
	reply.DoneSeq = px.done[px.me]
	if args.N > instance.Np {
		px.instances[args.Seq] = State{args.N, instance.Na, instance.Va, instance.Done}
		reply.N_a = instance.Na
		reply.V_a = instance.Va
		reply.Status = PrepareOK
	} else {
		// can't accept
		reply.Status = PrepareErr
	}
	return nil
}

func (px *Paxos) Prepare(idx int, p string, args *PrepareArgs, reply *PrepareReply) bool {
	if idx != px.me {
		return call(p, "Paxos.PrepAcceptor", args, reply)
	} else {
		// call local function
		return px.PrepAcceptor(args, reply) == nil
	}
}

func (px *Paxos) AcceptAcceptor(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	instance := px.instances[args.Seq]
	// update
	reply.DoneSeq = px.done[px.me]
	if args.N >= instance.Np {
		px.instances[args.Seq] = State{args.N, args.N, args.V, instance.Done}
		reply.Status = AcceptOK
	} else {
		// can't accept
		reply.Status = AcceptErr
	}
	return nil
}

func (px *Paxos) Accept(idx int, p string, args *AcceptArgs, reply *AcceptReply) bool {
	if idx != px.me {
		return call(p, "Paxos.AcceptAcceptor", args, reply)
	} else {
		// call local function
		return px.AcceptAcceptor(args, reply) == nil
	}
}

func (px *Paxos) DecideAcceptor(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	instance := px.instances[args.Seq]
	// update
	reply.DoneSeq = px.done[px.me]
	if args.N >= instance.Np {
		instance.Na = args.N
		instance.Np = args.N
	}
	instance.Va = args.V
	instance.Done = true
	px.instances[args.Seq] = instance
	reply.Status = DecideOK
	return nil
}

func (px *Paxos) Decide(idx int, p string, args *DecideArgs, reply *DecideReply) bool {
	if idx != px.me {
		return call(p, "Paxos.DecideAcceptor", args, reply)
	} else {
		// call local function
		return px.DecideAcceptor(args, reply) == nil
	}
}

// Proposer, as according to the assignment pseudocode
func (px *Paxos) Proposer(seq int, v interface{}) {
	// n starts at 0
	n := 0
	//decided := false
	majoritySize := len(px.peers) / 2
  	decided := false
	for !decided && !px.dead {
		agreedPeers := 0
		highestN := -1
		highestV := v
		// prepare
		for i, p := range px.peers {
			args := &PrepareArgs{seq, n}
			var reply PrepareReply
			prepareResponse := px.Prepare(i, p, args, &reply)
			if prepareResponse && reply.Status == PrepareOK {
				// piggyback the done sequence number
				px.mu.Lock()
				if px.done[i] < reply.DoneSeq {
					px.done[i] = reply.DoneSeq
				}
				px.mu.Unlock()
				if reply.N_a > highestN {
					highestN = reply.N_a
					highestV = reply.V_a
				}
				agreedPeers++
			}
		}
		if agreedPeers <= majoritySize {
			// no consensus, update n
			n = getMax(highestN, n) + 1
			// random backoff
			rand.Seed(time.Now().UnixNano())
			r := rand.Intn(100)
			time.Sleep(time.Duration(r) * time.Millisecond)
			continue
		}
		// accept
		agreedPeers = 0
		for i, p := range px.peers {
			args := &AcceptArgs{seq, n, highestV}
			var reply AcceptReply
			prepareResponse := px.Accept(i, p, args, &reply)
			if prepareResponse && reply.Status == AcceptOK {
				// piggyback the done sequence number
				px.mu.Lock()
				if px.done[i] < reply.DoneSeq {
					px.done[i] = reply.DoneSeq
				}
				px.mu.Unlock()
        		agreedPeers++
			}
		}
		if agreedPeers <= majoritySize {
			// no consensus
			n = getMax(highestN, n) + 1
			// random backoff
			rand.Seed(time.Now().UnixNano())
			r := rand.Intn(100)
			time.Sleep(time.Duration(r) * time.Millisecond)
			continue
		}
		decided = true
		//decide
		for i, p := range px.peers {
			args := &DecideArgs{seq, n, highestV}
			var reply DecideReply
			decideResponse := px.Decide(i, p, args, &reply)
			if decideResponse && reply.Status == DecideOK {
				// piggyback the done sequence number
				px.mu.Lock()
				if px.done[i] < reply.DoneSeq {
					px.done[i] = reply.DoneSeq
				}
				px.mu.Unlock()
			}
		}
	}
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	if seq < px.Min() {
		return
	}
	go func() {
		px.Proposer(seq, v)
	}()
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
  px.done[px.me] = getMax(px.done[px.me], seq)
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	return px.maxSeqNum
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	m := px.done[px.me]
	for _, v := range px.done {
		if v < m {
			m = v
		}
	}
	for seq, instance := range px.instances {
		if seq <= m && instance.Done {
			delete(px.instances, seq)
		}
	}
	return m + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// return done, value
	// if Status() is called with a seq num < Min(), meaning no agreement, thus return false
	if seq < px.Min() {
		return false, nil
	}
  	px.mu.Lock()
  	defer px.mu.Unlock()
  	_, exist := px.instances[seq]
  	if exist {
    	px.maxSeqNum = getMax(seq, px.maxSeqNum)
  	} else {
    	px.instances[seq] = State{-1, -1, nil, false}
  	}
  	instance := px.instances[seq]
  	return instance.Done, instance.Va
}

// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.maxSeqNum = -1
	px.instances = make(map[int]State)
	px.done = make(map[int]int)
	// -1 as seq num means that Done() was never called for this peer
	for i := 0; i < len(px.peers); i++ {
		px.done[i] = -1
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
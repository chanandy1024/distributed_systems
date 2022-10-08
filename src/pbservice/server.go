package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  // mutex for concurrent access to map
  mu sync.Mutex
  view viewservice.View
  dataMap map[string]string
  randMap map[int64]string
  // makes sure all the replicate requests are done before exiting
  replicateDone sync.WaitGroup
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()	
  pb.replicateDone.Wait()	
	pb.replicateDone.Add(1)	
  // check if key already exists in the map
  val, exist := pb.randMap[args.Rand]
  if exist {
  	reply.PreviousValue = val
    pb.replicateDone.Done()
  	pb.mu.Unlock()
  	return nil
  }
  var tmpVal string
  if args.DoHash {
  	tmpVal, exist = pb.dataMap[args.Key]
  	if !exist {
  		tmpVal = ""
  	}
  	args.Value = strconv.Itoa(int(hash(tmpVal + args.Value)))
  }
  pb.dataMap[args.Key] = args.Value
  pb.randMap[args.Rand] = tmpVal
  reply.PreviousValue = tmpVal
  pb.mu.Unlock()

  if pb.view.Backup == "" {
    pb.replicateDone.Done()
    return nil
  }
  reply.Err = OK
  if pb.view.Primary == pb.me {
  	// forward put to backup server
  	args.DoHash = false
  	ok := false
  	for ok==false {
      if pb.view.Backup == "" {
        pb.replicateDone.Done()
        reply.Err = ErrWrongServer
        return nil
      }
	    ok = call(pb.view.Backup, "PBServer.Put", args, &reply)
  	}
    pb.replicateDone.Done()
  } else if pb.view.Backup == pb.me {
    pb.replicateDone.Done()
  	reply.Err = ErrWrongServer
  } else {
    reply.Err = ErrWrongServer
    pb.replicateDone.Done()
  }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()
  var exist bool
  if pb.view.Primary == pb.me {
  	reply.Value, exist = pb.dataMap[args.Key]
  	if !exist {
  		reply.Err = ErrNoKey
  	}
  } else {
  	reply.Err = ErrWrongServer
  }
  return nil
}


func (pb *PBServer) CopyToBackup(args *NewBackupArgs, reply *NewBackupReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.dataMap = args.DataMap
	pb.randMap = args.RandMap
	reply.Value = OK
	return nil
}
// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  incomingView, _ := pb.vs.Ping(pb.view.Viewnum)
  // check if backup is new
  if incomingView.Primary == pb.me && incomingView.Backup != "" && incomingView.Backup != pb.view.Backup {
    var reply NewBackupReply
    pb.mu.Lock()
    // keep trying until success
    ok := false
    for ok == false {
      args := NewBackupArgs{pb.dataMap, pb.randMap}
      ok = call(incomingView.Backup, "PBServer.CopyToBackup", args, &reply)
      if pb.view.Backup == "" {
        break
      }
    }
    pb.mu.Unlock()
  }
  pb.view = incomingView
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
  pb.dataMap = make(map[string]string)
  pb.randMap = make(map[int64]string)
  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
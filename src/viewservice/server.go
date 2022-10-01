
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string


  // Your declarations here.
  namesToTimesMap map[string]time.Time
  curView View
  acked bool
  idleServer string
}

//
// reassign the servers
//
func (vs *ViewServer) reassign(primary string, backup string) {
  vs.curView.Primary = primary
  vs.curView.Backup = backup
}

//
// change views
//
func (vs *ViewServer) changeView() {
  vs.acked = false
  vs.curView.Viewnum ++
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.

  pingedServer, pingedViewNum := args.Me, args.Viewnum
  currentTime := time.Now()

  // update latest ping time
  vs.namesToTimesMap[pingedServer] = currentTime

  if pingedServer == vs.curView.Primary {
    if pingedViewNum == vs.curView.Viewnum {
      vs.acked = true
    } else if pingedViewNum == 0 {
      // primary restarted
      vs.reassign(vs.curView.Backup, vs.idleServer)
      vs.changeView()
    }
  } else if pingedServer == vs.curView.Backup {
    // backup restarted
    if pingedViewNum == 0 && vs.acked {
      vs.reassign(vs.curView.Primary, vs.idleServer)
      vs.changeView()
    }
  } else {
    // the first ping
    if vs.curView.Viewnum == 0 {
      vs.reassign(pingedServer, "")
      vs.changeView()
    } else {
        vs.idleServer = pingedServer
      }
  }
  reply.View = vs.curView
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  reply.View = vs.curView
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Your code here.
  // vs.mu.Lock()
  // defer vs.mu.Unlock()
  if (time.Since(vs.namesToTimesMap[vs.idleServer]) >= DeadPings*PingInterval) {
    vs.idleServer = ""
  }
  if !vs.acked {
    return
  }
  if (time.Since(vs.namesToTimesMap[vs.curView.Primary]) >= DeadPings*PingInterval) {
    vs.reassign(vs.curView.Backup, vs.idleServer)
    vs.changeView()
    vs.idleServer = ""
  }
  if (time.Since(vs.namesToTimesMap[vs.curView.Backup]) >= DeadPings*PingInterval) {
    if vs.idleServer != "" {
      vs.reassign(vs.curView.Primary, vs.idleServer)
      vs.changeView()
      vs.idleServer = ""
    }
  }
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.curView = View{0, "", ""}
  vs.acked = false
  vs.idleServer = ""
  vs.namesToTimesMap = make(map[string]time.Time)
  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}

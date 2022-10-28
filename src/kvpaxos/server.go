package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  ClientId int64
  SeqNum int64
  Key string
  Value string
  OpType int // 0 for GET, 1 for PUT
  DoHash bool
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  kvData map[string]string
  prevVal map[int64]string
  finished map[int64]bool
  seqLog int
}

//Wait for the status
func (kv *KVPaxos) Wait(seq int) Op {
	to := 10 * time.Millisecond
	for {
		decided, v := kv.px.Status(seq)
		if decided {
			return v.(Op)
    }
		time.Sleep(to)
		if to < 10 * time.Second {
			to *= 2
		}
	}
}

// Process the incoming request.
// Update the database and make the new operation.
// Avoid make double modification.
func (kv *KVPaxos) Update(op Op) string {
  for {
    seq := kv.seqLog + 1
    var nextOp Op
    decided, val := kv.px.Status(seq)
    if decided {
			nextOp = val.(Op)
		} else {
			kv.px.Start(seq, op) // discover the previously agreed-to value, or cause agreement to happen
			nextOp = kv.Wait(seq)
		}
    curVal := kv.kvData[nextOp.Key]
    if nextOp.OpType == 1 {
      if nextOp.DoHash {
        kv.kvData[nextOp.Key] = strconv.Itoa(int(hash(curVal + nextOp.Value)))
        kv.prevVal[nextOp.ClientId] = curVal
        kv.finished[nextOp.SeqNum] = true
      } else {
			  kv.kvData[nextOp.Key] = nextOp.Value
        kv.finished[nextOp.SeqNum] = true
      }
    }
    kv.seqLog++
    kv.px.Done(kv.seqLog)
    if op == nextOp {
      return curVal
    }
  }
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  op := Op{args.ClientId, args.SeqId, args.Key, "", 0, false}
  v := kv.Update(op)
  reply.Value = v
  reply.Err = OK
  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  _, exist := kv.finished[args.SeqId]
  if exist {
		if args.DoHash {
			reply.PreviousValue = kv.prevVal[args.ClientId]
		}
		reply.Err = OK
    return nil
  }

	op := Op{args.ClientId, args.SeqId, args.Key, args.Value, 1, args.DoHash}
  v := kv.Update(op)
  reply.PreviousValue = v
  reply.Err = OK
  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me

  // Your initialization code here.
  kv.kvData = make(map[string]string)
  kv.finished = make(map[int64]bool)
  kv.prevVal = make(map[int64]string)
  kv.seqLog = -1

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}
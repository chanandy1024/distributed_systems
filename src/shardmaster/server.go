package shardmaster

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

const (
  J = "Join"
  L = "Leave"
  M = "Move"
  Q = "Query"
)

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  
  configs []Config // indexed by config num
  seq int // current sequence number
}


type Op struct {
  // Your data here.
  Operation string // Join, Leave, Move, Query
  Args OpArgsStruct // JoinArgs, LeaveArgs, MoveArgs, QueryArgs
}

type OpArgsStruct struct {
  *JoinArgs
  *LeaveArgs
  *MoveArgs
  *QueryArgs
}

//Wait for the status
func (sm *ShardMaster) Wait(seq int) Op {
  to := 10 * time.Millisecond
  for {
    decided, v := sm.px.Status(seq)
    if decided {
      return v.(Op)
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }
}
func (sm *ShardMaster) Balance(config *Config) {
	// count how many shards are in each group
	counts := make(map[int64]int)
  for i := 0; i < NShards; i++ {
    counts[config.Shards[i]]++
  }
  averageShardsPerGroup := NShards/len(config.Groups)
  var min_count int
  var min_group int64
  for i := 0; i < NShards; i++ {
    min_count = -1
    min_group = -1
    // find the group with the smallest number of shards
    // enter if shards in invalid group or the counts of the group is too big
    if config.Shards[i] == 0 || counts[config.Shards[i]] > averageShardsPerGroup {
      for group := range config.Groups {
        // find the smallest group and assign current shard to it
        if min_group == -1 || counts[group] < min_count {
          min_group = group
          min_count = counts[group]
        }
      }
      // if min found is already at the average shard level and the group isn't invalid, then simply continue
      if config.Shards[i] != 0 && counts[min_group] == averageShardsPerGroup {
        continue
      }
      counts[min_group]++
      counts[config.Shards[i]]--
			config.Shards[i] = min_group
    }
  }
}

func (sm *ShardMaster) ProcessLog(op Op) {
  curConfig := sm.configs[len(sm.configs) - 1]
  var newConfig Config
  // initialize num and groups
	newConfig.Num = len(sm.configs)
	newConfig.Groups = make(map[int64][]string)
  // copy over shards and groups
  for i := 0; i < NShards; i++ {
    newConfig.Shards[i] = curConfig.Shards[i]
  }
  for i, server := range curConfig.Groups {
    newConfig.Groups[i] = server
  }
  switch op.Operation {
  case J:
    args := op.Args.JoinArgs
    newConfig.Groups[args.GID] = args.Servers
    sm.Balance(&newConfig)
  case M:
    args := op.Args.MoveArgs
    newConfig.Shards[args.Shard] = args.GID
  case L:
    args := op.Args.LeaveArgs
    // https://edstem.org/us/courses/28657/discussion/2088917
    // assign all the shards to invalid group 0
    for i := 0; i < NShards; i++ {
      if newConfig.Shards[i] == args.GID {
        newConfig.Shards[i] = 0
      }
    }
    delete(newConfig.Groups, args.GID)
    sm.Balance(&newConfig)
  }
  if op.Operation != Q {
    sm.configs = append(sm.configs, newConfig)
  }
}
// Process the incoming request.
// Update the database and make the new operation.
// Avoid make double modification.
func (sm *ShardMaster) Update(op Op) {
  for {
    seq := sm.seq + 1
    var nextOp Op
    decided, val := sm.px.Status(seq)
    if decided {
			nextOp = val.(Op)
		} else {
			sm.px.Start(seq, op) // discover the previously agreed-to value, or cause agreement to happen
			nextOp = sm.Wait(seq)
		}
    if nextOp.Operation != Q {
      sm.ProcessLog(nextOp) 
    }
    sm.seq++
    sm.px.Done(sm.seq)
    if op == nextOp {
      return
    }
  }
}

/*
The shardmaster should react by creating a new configuration that includes the new replica group. 
The new configuration should divide the shards as evenly as possible among the groups, and should move 
as few shards as possible to achieve that goal.
*/
func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{J, OpArgsStruct{JoinArgs: args}}
  sm.Update(op)
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{L, OpArgsStruct{LeaveArgs: args}}
  sm.Update(op)
  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{M, OpArgsStruct{MoveArgs: args}}
  sm.Update(op)
  return nil
}

/*
The Query RPC’s argument is a configuration number. The shardmaster replies with the configuration that 
has that number. If the number is -1 or bigger than the biggest known configuration number, the 
shardmaster should reply with the latest configuration. The result of Query(-1) should reflect every 
Join, Leave, or Move that completed before the Query(-1) RPC was sent.
*/

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{Q, OpArgsStruct{QueryArgs: args}}
  sm.Update(op)
  if args.Num == -1 || args.Num >= len(sm.configs) {
    reply.Config = sm.configs[len(sm.configs)-1]
    return nil
  }

  // finding config with the config number
  for _, config := range sm.configs {
    if config.Num == args.Num {
      reply.Config = config
      return nil
    }
  }
  reply.Config = sm.configs[len(sm.configs)-1]
  return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  
  sm := new(ShardMaster)
  sm.me = me
  
  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  sm.seq = 0
  rpcs := rpc.NewServer()
  rpcs.Register(sm)
  
  sm.px = paxos.Make(servers, me, rpcs)
  
  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l
  
  // please do not change any of the following code,
  // or do anything to subvert it.
  
  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
          } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
            if err != nil && sm.dead == false {
              fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
              sm.Kill()
            }
          }
          }()
          
          return sm
        }
        
package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) ProposeAcceptor(message base.Message, nodes []base.Node) []base.Node {
	proposedMsg, exist := message.(*ProposeRequest)
	if !exist {
		fmt.Println("Message can't be read")
		return nodes
	}
	// based on hw3 code
	updatedNode := server.copy()
	var res ProposeResponse
	if proposedMsg.N > server.n_p {
		// If N > Np then: Update Np = N, Reply <PROPOSE-OK, Na, Va>
		updatedNode.n_p = proposedMsg.N
		// update and return 
		res = ProposeResponse{
			base.MakeCoreMessage(message.To(), message.From()),
			true,
			updatedNode.n_p,
			updatedNode.n_a,
			updatedNode.v_a,
			proposedMsg.SessionId}
		updatedNode.SetSingleResponse(&res)
		nodes = append(nodes, updatedNode)
	} else {
		// can't accept
		res = ProposeResponse{
			base.MakeCoreMessage(message.To(), message.From()),
			false,
			updatedNode.n_p,
			updatedNode.n_a,
			updatedNode.v_a,
			proposedMsg.SessionId}
		updatedNode.SetSingleResponse(&res)
		nodes = append(nodes, updatedNode)
	}
	return nodes
}

func (server *Server) DecideAcceptor(message base.Message, nodes []base.Node) []base.Node {
	proposedMsg, exist := message.(*DecideRequest)
	if !exist {
		fmt.Println("Message can't be read")
		return nodes
	}
	updatedNode := server.copy()
	updatedNode.agreedValue = proposedMsg.V
	// Note: no need to respond to a Decide Message, thus directly append
	nodes = append(nodes, updatedNode)
	return nodes
}

func (server *Server) AcceptAcceptor(message base.Message, nodes []base.Node) []base.Node {
	proposedMsg, exist := message.(*AcceptRequest)
	if !exist {
		fmt.Println("Message can't be read")
		return nodes
	}
	updatedNode := server.copy()
	var res AcceptResponse
	// If N >= Np then: Update Np = N, Na = N, Va = V, Reply <ACCEPT-OK>
	if proposedMsg.N >= updatedNode.n_p {
		updatedNode.n_p = proposedMsg.N
		updatedNode.n_a = proposedMsg.N
		updatedNode.v_a = proposedMsg.V
		updatedNode.proposer.SessionId = proposedMsg.SessionId
		res = AcceptResponse{
			base.MakeCoreMessage(message.To(), message.From()),
			true,
			updatedNode.n_p,
			proposedMsg.SessionId}
		updatedNode.SetSingleResponse(&res)
		nodes = append(nodes, updatedNode)
	} else {
		// can't accept
		updatedNode.proposer.SessionId = proposedMsg.SessionId
		res = AcceptResponse{
			base.MakeCoreMessage(message.To(), message.From()),
			false,
			updatedNode.n_p,
			proposedMsg.SessionId}
		updatedNode.SetSingleResponse(&res)
		nodes = append(nodes, updatedNode)
	}
	return nodes
}

// first implement the acceptors
func (server *Server) Acceptor(message base.Message) []base.Node {
	nodes := make([]base.Node, 0)
	switch message.(type) {
	case *ProposeRequest:
		return server.ProposeAcceptor(message, nodes)
	case *DecideRequest:
		return server.DecideAcceptor(message, nodes)
	case *AcceptRequest:
		return server.AcceptAcceptor(message, nodes)
	}
	return nodes
}

func (server *Server) AcceptProposer(message base.Message, nodes []base.Node) []base.Node {
	// return the states
	msgResponse, exist := message.(*ProposeResponse)
	if !exist {
		fmt.Println("Message can't be read")
		return nodes
	}
	numPeers := len(server.peers)
	majoritySize := numPeers / 2
	// only run the process for the right session
	if server.proposer.SessionId == msgResponse.SessionId {
		if !msgResponse.Ok {
			node := server.copy()
			node.proposer.Phase = Propose
			node.proposer.ResponseCount++
			nodes = append(nodes, node)
		} else {
			agreedPeers := server.proposer.SuccessCount
			for i, p := range(server.peers) {
				if server.proposer.Responses[i] == false && msgResponse.From() == p {
					agreedPeers++
				}
			}
			// if reached consensus, may wait for the rest responses or enter the next phase
			if agreedPeers > majoritySize {
				node := server.copy()
				node.proposer.Phase = Accept
				// if no v_a is received
				if base.IsNil(msgResponse.V_a) && base.IsNil(node.proposer.V) {
					node.proposer.V = node.proposer.InitialValue
				} else if msgResponse.N_a > node.proposer.N_a_max && !base.IsNil(msgResponse.V_a) {
					// highest number N
					node.proposer.N_a_max = msgResponse.N_a
					node.proposer.V = msgResponse.V_a
				}
				// reset node
				node.proposer.SuccessCount = 0
				node.proposer.ResponseCount = 0
				node.proposer.Responses = make([]bool, len(node.peers))
				// send ProposeRequest to all its peers, including itself
				res := make([]base.Message, 0)
				for _, p := range(node.peers) {
					res = append(res, &AcceptRequest{
						base.MakeCoreMessage(node.Address(), p),
						node.proposer.N,
						node.proposer.V,
						node.proposer.SessionId})
				}
				node.SetResponse(res)
				nodes = append(nodes, node)
			} 
			if agreedPeers < numPeers && server.proposer.ResponseCount < numPeers - 1 {
				node := server.copy()
				node.proposer.ResponseCount ++
				if base.IsNil(msgResponse.V_a) && base.IsNil(node.proposer.V) {
					node.proposer.V = node.proposer.InitialValue
				} else if msgResponse.N_a > node.proposer.N_a_max && !base.IsNil(msgResponse.V_a) {
					// highest number N
					node.proposer.N_a_max = msgResponse.N_a
					node.proposer.V = msgResponse.V_a
				}
				// loop over peers to see which ones match
				for i, p := range(node.peers) {
					if node.proposer.Responses[i] == false && msgResponse.From() == p {
						node.proposer.SuccessCount++
						node.proposer.Responses[i] = true
					}
				}
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

func (server *Server) DecideProposer(message base.Message, nodes []base.Node) []base.Node {
	// return the states
	msgResponse, exist := message.(*AcceptResponse)
	if !exist {
		fmt.Println("Message can't be read")
		return nodes
	}
	numPeers := len(server.peers)
	majoritySize := numPeers / 2
	// only run the process for the right session
	if server.proposer.SessionId == msgResponse.SessionId {
		if !msgResponse.Ok {
			node := server.copy()
			node.proposer.Phase = Accept
			node.proposer.ResponseCount++
			nodes = append(nodes, node)
		} else {
			agreedPeers := server.proposer.SuccessCount
			for i, p := range(server.peers) {
				if server.proposer.Responses[i] == false && msgResponse.From() == p {
					agreedPeers++
				}
			}
			// if reached consensus, may wait for the rest responses or enter the next phase
			if agreedPeers > majoritySize {
				node := server.copy()
				node.proposer.Phase = Decide
				node.agreedValue = node.proposer.V
				// reset node
				node.proposer.SuccessCount = 0
				node.proposer.ResponseCount = 0
				node.proposer.Responses = make([]bool, len(node.peers))
				// send ProposeRequest to all its peers, including itself
				res := make([]base.Message, 0)
				for _, p := range(node.peers) {
					res = append(res, &DecideRequest{
						base.MakeCoreMessage(node.Address(), p),
						node.proposer.V,
						msgResponse.SessionId})
				}
				node.SetResponse(res)
				nodes = append(nodes, node)
			} 
			if agreedPeers < numPeers && server.proposer.ResponseCount < numPeers - 1 {
				node := server.copy()
				node.proposer.ResponseCount ++
				// loop over peers to see which ones match
				for i, p := range(node.peers) {
					if node.proposer.Responses[i] == false && msgResponse.From() == p {
						node.proposer.SuccessCount++
						node.proposer.Responses[i] = true
					}
				}
				nodes = append(nodes, node)
			}
		}
	}
	return nodes
}

// implement the proposer
func (server *Server) Proposer(message base.Message) []base.Node {
	nodes := make([]base.Node, 0)
	switch message.(type) {
	case *ProposeResponse:
		return server.AcceptProposer(message, nodes)
	case *AcceptResponse:
		return server.DecideProposer(message, nodes)
	}
	return nodes
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	//TODO: implement it
	switch message.(type) {
	case *ProposeRequest, *AcceptRequest, *DecideRequest:
		return server.Acceptor(message)
	case *ProposeResponse, *AcceptResponse:
		return server.Proposer(message)
	}
	return []base.Node{}
}

// To start a new round of Paxos.
/*
It is the starting point for proposing a value for consensus. 
The proposed value is InitialValue if no v_a is received. The first task of this function 
is to renew the proposer’s fields. Then send ProposeRequest to all its peers, including itself.
*/
func (server *Server) StartPropose() {
	// if no v_a is received
	if base.IsNil(server.v_a) && base.IsNil(server.proposer.V) {
		server.proposer.V = server.proposer.InitialValue
	}
	// renew fields
	server.proposer.Phase = Propose
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	server.proposer.Responses = make([]bool, len(server.peers))
	server.proposer.N++
	server.proposer.SessionId++
	// send ProposeRequest to all its peers, including itself
	res := make([]base.Message, 0)
	for _, p := range(server.peers) {
		res = append(res, &ProposeRequest{
			base.MakeCoreMessage(server.Address(), p),
			server.proposer.N,
			server.proposer.SessionId})
	}
	server.SetResponse(res)
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}

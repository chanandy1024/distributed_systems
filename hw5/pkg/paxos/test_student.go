package paxos

import (
	"coms4113/hw5/pkg/base"
)

// Fill in the function to lead the program to a state where A2 rejects the Accept Request of P1
/*
You are required to fill in a list of predicates so that the program first has A2 rejects P1 and
then have S3 be the first server knowing a consensus is reached. Your predicates should be completed
in the function ToA2RejectP1 and ToConsensusCase5 at test_student.go.
Meaning S3 will be the proposer where consensus is reached first
*/

func ToA2RejectP1() []func(s *base.State) bool {
	// referencing checksForPartition2
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}
	// p3 should also send out propose message after p1
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	}
	// check that s2 receives proposed N from p3
	p2Receivesp3Propose := func(s *base.State) bool {
		s2 := s.Nodes()["s2"].(*Server)
		s3 := s.Nodes()["s2"].(*Server)
		return s3.proposer.N >= s2.n_p
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	}
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	}
	// one of the acceptors will reject p1 because it has seen the larger Np value
	p1ReceiveReject := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.SuccessCount == 1
	}
	return []func(s *base.State) bool{p1PreparePhase, p3PreparePhase, p2Receivesp3Propose, 
		p1AcceptPhase, p3AcceptPhase, p1ReceiveReject}
}

// Fill in the function to lead the program to a state where a consensus is reached in Server 3.
func ToConsensusCase5() []func(s *base.State) bool {
	p3KnowConsensus := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		s3 := s.Nodes()["s3"].(*Server)
		return s1.proposer.Phase == Accept && s3.agreedValue == "v3" && s1.agreedValue != "v3"
	}
	return []func(s *base.State) bool{p3KnowConsensus}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected
func NotTerminate1() []func(s *base.State) bool {
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	}
	// p3 should also send out propose message after p1
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	}
	p3ReceivedFirstResponse := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.ResponseCount == 1
	}
	p3ReceivedSecondResponse := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.ResponseCount == 2
	}
	p3ReceivedThirdResponse := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose && s3.proposer.ResponseCount == 3
	}
	p1Rejected1 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 1 && s1.proposer.SuccessCount == 0
	}
	p1Rejected2 := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.ResponseCount >= 2 && s1.proposer.SuccessCount == 0
	}
	return []func(s *base.State) bool{p1PreparePhase, p3PreparePhase,
		p1AcceptPhase,
		p3ReceivedFirstResponse, p3ReceivedSecondResponse, p3ReceivedThirdResponse,
		p1Rejected1, p1Rejected2}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P3 are rejected
func NotTerminate2() []func(s *base.State) bool {
	p1PrepareAgain := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}
	p1FirstResponse := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return  s1.proposer.Phase == Propose && s1.proposer.ResponseCount == 1
	}
	p1SecondResponse := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.ResponseCount == 2
	}
	p1ThirdResponse := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose && s1.proposer.ResponseCount == 3
	}
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	}
	p3FirstReject := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.ResponseCount == 1 && s3.proposer.SuccessCount == 0
	}
	p3SecondRejected := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.ResponseCount == 2 && s3.proposer.SuccessCount == 0
	}
	return []func(s *base.State) bool{p1PrepareAgain, p1FirstResponse,
		p1SecondResponse, p1ThirdResponse, p3AcceptPhase, 
		p3FirstReject, p3SecondRejected}
}

// Fill in the function to lead the program to a state where all the Accept Requests of P1 are rejected again.
func NotTerminate3() []func(s *base.State) bool {
	return NotTerminate1()
}

// Fill in the function to lead the program to make P1 propose first, then P3 proposes, but P1 get rejects in
// Accept phase
func concurrentProposer1() []func(s *base.State) bool {
	// referencing checksForPartition2
	p1PreparePhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Propose
	}
	// p3 should also send out propose message after p1
	p3PreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	}
	p1AcceptPhase := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept
	}
	// one of the acceptors will reject p1 because it has seen the larger Np value
	p1ReceivesFirstReject := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.ResponseCount == 1 && s1.proposer.SuccessCount == 0
	}
	p1ReceivesSecondReject := func(s *base.State) bool {
		s1 := s.Nodes()["s1"].(*Server)
		return s1.proposer.Phase == Accept && s1.proposer.ResponseCount >= 2 && s1.proposer.SuccessCount == 0
	}
	return []func(s *base.State) bool{p1PreparePhase, p3PreparePhase, 
		p1AcceptPhase, p1ReceivesFirstReject, p1ReceivesSecondReject}
}

// Fill in the function to lead the program continue P3's proposal and reaches consensus at the value of "v3".
func concurrentProposer2() []func(s *base.State) bool {
	p3StillInPreparePhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Propose
	}
	p3ReceivesFirstResponseSuccess := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.ResponseCount == 1 && s3.proposer.SuccessCount == 1
	}
	p3ReceivesMajorityResponseSuccess:= func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.ResponseCount >= 2 && s3.proposer.SuccessCount >= 2
	}
	// after receiving responses, enter accept phase
	p3AcceptPhase := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept
	}
	p3ReceiveSMajorityAcceptOk := func(s *base.State) bool {
		s3 := s.Nodes()["s3"].(*Server)
		return s3.proposer.Phase == Accept && s3.proposer.ResponseCount >= 2 && s3.proposer.SuccessCount >= 2
	}

	return []func(s *base.State) bool{
		p3StillInPreparePhase,
		p3ReceivesFirstResponseSuccess,
		p3ReceivesMajorityResponseSuccess,
		p3AcceptPhase,
		p3ReceiveSMajorityAcceptOk}
}

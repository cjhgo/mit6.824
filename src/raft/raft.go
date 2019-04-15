package raft

import "time"
import "math/rand"
//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import "sync"
import "labrpc"

// import "bytes"
// import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Term int	
}

type AppendEntriesArgs struct{
	Entry ApplyMsg
	Term int
}

type AppendEntriesReply struct{
	
}
//
// A Go object implementing a single Raft peer.
//
type RaftState int
const (
	Leader RaftState = 0
	Follower RaftState = 1
	Candidate RaftState = 2
)
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	role			RaftState						// this peer 处以哪个状态,Leader/Follower/Candidate
	voteFor 	int									// this peer 是否已经投过票了
	voteCnt 	int 								// this peer 在选举中获得了多少票数
	currentTerm		int							// this peer 当前处于哪个term
	currentIndex	int							// tiis peer 的logs最新的log entry的index	
	n					int									// peers中一共有多少个peer
	electionTimer *time.Timer 			//选举计时器
	heartTimer *time.Timer 				//心跳等待计时器
	heartTicker *time.Ticker 				//心跳等待计时器
	applyCh chan ApplyMsg
	recVote chan string //收到了投票请求
	recHeart chan string //收到了心跳包
	ticker *time.Ticker
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.role == Leader)
	return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}





//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	Peer int


}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	Result bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	go func(){rf.recVote <- ""}()
	// Your code here (2A, 2B).
	rf.mu.Lock()	
	if args.Term < rf.currentTerm{
		reply.Result = false
	}else if args.Term == rf.currentTerm{
		if rf.voteFor == -1{
			reply.Result = true
			rf.voteFor = args.Peer
		}else{
			reply.Result = false
		}
	}else if args.Term > rf.currentTerm{
		reply.Result = true
		rf.voteFor = args.Peer
		DPrintf("raft %v change  %v,%v --> %v,%v,cause get vote req from %v", rf.me, rf.role,rf.currentTerm,1,args.Term,args.Peer)
		rf.currentTerm = args.Term		
		rf.applyCh <- ApplyMsg{}
		rf.role = Follower
		if rf.ticker != nil{
			rf.ticker.Stop()
		}		
	}
	rf.mu.Unlock()
	// if rf.voteFor != -1 || args.Term < rf.currentTerm{
	// 	reply.Result = false
	// }else{		
	// 	reply.Result = true
	// 	rf.voteFor = args.Peer
	// }
}

func (rf *Raft) AppendEntries(args* AppendEntriesArgs, reply *AppendEntriesReply){
	go func(){rf.recHeart <- ""}()
	rf.applyCh <- args.Entry
}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("raft %v req vote to %v : %v",rf.me,server, ok)
	if reply.Result {
		rf.mu.Lock()
		rf.voteCnt++
		rf.mu.Unlock()
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)	
	DPrintf("raft %v sendAppendEnitries to %v :%v", rf.me, server, ok)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.n = len(peers)
	rf.currentTerm = 0
	rf.voteCnt = 0
	rf.voteFor = -1
	rf.applyCh = applyCh
	rf.recVote = make(chan string)
	rf.recHeart = make(chan string)
	rf.ticker = time.NewTicker(40*time.Millisecond)
	heart :=  120*time.Millisecond		
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	
	// heartmap := map[int]int{
	// 	0:150,
	// 	1:160,
	// 	2:170,
	// }
	// heart := time.Duration(heartmap[rf.me]) * time.Millisecond


	go func(){		
		for{			
			if rf.role == Follower{
				DPrintf("raft %v begin check heartbeat and selection,with role %v at %v",rf.me, rf.role, rf.currentTerm)
				DPrintf("raft %v will wait heart for %v", rf.me, heart)
				// heartTimeout := make(chan string)
				// go func(){
				// 	rf.heartTimer.Reset(heart) 
				// 	//= time.NewTimer(heart)	
				// 	<- rf.heartTimer.C
				// 	heartTimeout <- "aaaaaaa!"
				// 	DPrintf("heartTimeout Happened! on raft: %v %v %v", rf.me, rf.role,rf.currentTerm)
				// }()
				// if rf.heartTimer == nil{
				// 	rf.heartTimer = time.NewTimer(heart)
				// }
				select{
					case <- rf.recHeart:
					case <- rf.recVote:
					case <- time.After(120*time.Millisecond):{
						DPrintf("raft %v be %v  at %v not receive heartbeat after %v", rf.me,rf.role,rf.currentTerm, heart)
						rf.role = Candidate
				}
					// case m := <- applyCh:
					// {
					// 	// DPrintf("raft %v receive heartmessage ,%v, %v",rf.me, m,rf.heartTimer.Reset(heart))
					// 	DPrintf("raft %v receive heartmessage ,%v",rf.me, m)
					// 	if !rf.heartTimer.Stop(){
					// 		<-rf.heartTimer.C
					// 	}						
					// 	rf.heartTimer.Reset(heart)
					// 	// <-rf.heartTimer.C
					// 	// rf.heartTimer = time.NewTimer(heart)
					// 	// rf.heartTimer.Reset(heart)
					// }					
					// // case s := <- heartTimeout:
					// case <-rf.heartTimer.C:
					// {
					// 	//DPrintf("raft %v at %v not receive heartbeat after %v, %v", rf.me,rf.role, heart,rf.heartTimer.Reset(heart))
						
						
					// }
				}
			}else if rf.role == Candidate{				
				rf.currentTerm++
				rf.voteCnt=1
				rf.voteFor=rf.me
				x := time.Duration(rand.Intn(80)+300) * time.Millisecond
				DPrintf("raft %v begin check heartbeat and selection,with role %v at %v,wait for %v",rf.me, rf.role, rf.currentTerm,x)
				rf.electionTimer = time.NewTimer(x)
				waitVote := make(chan string,2)
				go func(){
					var wg sync.WaitGroup
					for i := 0; i < rf.n; i++{
						if i != rf.me{
							wg.Add(1)
							go func(i int){
								req := &RequestVoteArgs{rf.currentTerm, rf.me}
								rep := &RequestVoteReply{}
								rf.sendRequestVote(i, req, rep)
								wg.Done()					
								DPrintf("raft %v %v at %v req vote to %v res : %v ->%v",rf.me, rf.role,rf.currentTerm, i,rep, rf.voteCnt)			
								if rf.voteCnt >= 2{
									waitVote<- "done"				
								}
							}(i)
						}
					}
					wg.Wait()
					waitVote<- "done"
				}()
				for{	
					DPrintf("raft %v begin candidate loop with role %v", rf.me, rf.role)
					timeout := false
					select{
						case  <- waitVote:{
							DPrintf("raft %v vote finish ,get vote  %v at %v", rf.me,rf.voteCnt, rf.currentTerm)
							if rf.voteCnt >= 2{
								rf.role = Leader
								DPrintf("wow, raft %v become a leader at %v ", rf.me,rf.currentTerm)
							}else{
								DPrintf("sad, raft %v not become a leader at %v %v", rf.me,rf.currentTerm,rf.voteCnt)
							}
						}
						case m := <- applyCh:{
							DPrintf("when candidate , raft %v receive %v ", rf.me, m)
							if m.Term >= rf.currentTerm {
								rf.role = Follower
								DPrintf("when candidate , raft %v become follower ", rf.me)
							}
						}
						case <- rf.electionTimer.C:{
							DPrintf("raft %v election timeout",rf.me)
							timeout = true
						}
					}
					if rf.role != Candidate || timeout {
						break
					}					
				}
			}else if rf.role == Leader{
				DPrintf("raft %v begin check heartbeat and selection,with role %v at %v",rf.me, rf.role, rf.currentTerm)
				for _ = range rf.ticker.C{			
					// m.Term = rf.currentTerm		
					for i := 0; i < rf.n; i++{
						if i != rf.me{
							// DPrintf("leader %v heart raft %v", rf.me, i)
							go func(i int){
								args := &AppendEntriesArgs{}
								entry := ApplyMsg{}
								entry.Term = rf.currentTerm
								args.Entry=entry
								reply := &AppendEntriesReply{}
								rf.sendAppendEntries(i, args, reply)
							}(i)
						}
					}
					// DPrintf("leader %v tick at %v",rf.me, t)
				}
			}
		}
	}()

	return rf
}

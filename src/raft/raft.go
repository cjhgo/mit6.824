package raft

import (
	"sync"
	"labrpc"
	"time"
	"math/rand"
	"math"
	"encoding/json"
	"log"
	_ "fmt"
)
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
	CommandTerm int
}

type LogEntry struct{
	Command interface{}
	Index int
	Term int
}
func logentryToapplymsg(entry *LogEntry) ApplyMsg{
	m := ApplyMsg{}
	m.CommandValid = true
	m.Command = entry.Command
	m.CommandIndex = entry.Index
	m.CommandTerm = entry.Term
	return m
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
	commitIndex	int							// tiis peer 的logs最新的log entry的index	
	lastApplied int							//上一个applied的log entry的index
	n					int									// peers中一共有多少个peer
	half int
	// electionTimer *time.Timer 			//选举计时器
	// heartTimer *time.Timer 				//心跳等待计时器
	// heartTicker *time.Ticker 				//心跳等待计时器
	applyCh chan ApplyMsg
	recVote chan string //收到了投票请求
	recHeart chan string //收到了心跳包
	electWin chan string //得到多数票,赢得了选举
	logs []LogEntry
	nextIndex []int
	// ticker *time.Ticker
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
	CandidateId int
	LastLogIndex int
	LastLogTerm int

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


func (rf *Raft)comparelog(index int ,term int) bool{
	if term > rf.logs[rf.commitIndex].Term {
		return true
	}else if term == rf.logs[rf.commitIndex].Term{
		if index >= rf.logs[rf.commitIndex].Index{
			return true
		}else{
			return false
		}
	}else{
		return false
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()	
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.Result = false
		return
	}else{
		if args.Term > rf.currentTerm{
			DPrintf("raft %v %v at %v change to follower",rf.me, rf.role,rf.currentTerm)			
			rf.role=Follower
			rf.voteFor=-1
			rf.currentTerm = args.Term			
		}
		reply.Result = false
		if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.comparelog(args.LastLogIndex,args.LastLogTerm) {
			reply.Result = true
			rf.recVote <- ""
			rf.voteFor = args.CandidateId
		}
	}
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
	if ok{
		rf.mu.Lock()
		if reply.Term > rf.currentTerm{
			rf.role=Follower
			rf.voteFor=-1
			rf.currentTerm=reply.Term
			DPrintf("raft %v at %v , here vote :%v",rf.me,rf.currentTerm,rf.voteCnt)
			return ok
		}
		if rf.role == Candidate && rf.currentTerm == args.Term{
			if reply.Result {
				rf.voteCnt++
				DPrintf("raft %v at %v , now vote :%v",rf.me,rf.currentTerm,rf.voteCnt)
			}		
			if rf.voteCnt >= rf.half{
				rf.electWin <- ""
			}
		}
		rf.mu.Unlock()
	}
	return ok
}


type AppendEntriesArgs struct{
	Entries []LogEntry
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	LeaderCommit int

}

func String(obj interface{})string{
	out,_:=json.Marshal(obj)
	return string(out)
}

type AppendEntriesReply struct{
	Term int
	Result bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)	
	DPrintf("raft %v sendAppendEnitries to %v :%v", rf.me, server, ok)
	if ok{
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm{
			rf.role=Follower
			rf.voteFor=-1
			rf.currentTerm=reply.Term
			return ok
		}
	}
	return ok
}

func (rf *Raft) AppendEntries(args* AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term=rf.currentTerm
	if args.Term < rf.currentTerm{
		reply.Result=false
		return
	}else{
		rf.recHeart <- ""
		if args.Term > rf.currentTerm{
			rf.role=Follower
			rf.voteFor=-1
			rf.currentTerm=reply.Term
			reply.Result=false
		}
		consistency := false
		// DPrintf("raft %v get command %v", rf.me,String(args))
		if args.LeaderCommit == 1{
			DPrintf("raft %v get command %v", rf.me,String(args))
		}
		if rf.logs[rf.commitIndex].Term == args.PrevLogTerm && rf.logs[rf.commitIndex].Index == args.PrevLogIndex{
			consistency=true
			DPrintf("raft %v consistency with raft %v", rf.me, args.LeaderId)
			if( rf.lastApplied < args.LeaderCommit){
				newlastApplied := args.LeaderCommit
				if rf.commitIndex < newlastApplied{
					newlastApplied = rf.commitIndex
				}
				for j:= rf.lastApplied+1; j <= newlastApplied; j++{
					entry := rf.logs[j]
					m := ApplyMsg{true,entry.Command,entry.Index,entry.Term}						
					rf.applyCh <- m
				}
				rf.lastApplied = newlastApplied
			}
		}
		if consistency==false{
			reply.Result = false			
		}else{
			for pos := range args.Entries{
				entry := args.Entries[pos]
				rf.commitIndex += 1
				rf.logs[rf.commitIndex]=entry
			}
			reply.Result=true
		}
		return
	}
}

func (rf *Raft) SendCommandToAll(command interface{}, index int,term int){
	AeCount := 1
	var mu sync.Mutex 
	for i := 0; i < rf.n; i++{
		if i != rf.me{
			go func(i int){
				entry := LogEntry{command, index, term}
				args := &AppendEntriesArgs{
					[]LogEntry{entry},
					term,
					rf.me,
					rf.logs[index-1].Index,
					rf.logs[index-1].Term,
					rf.lastApplied,
				}
				reply := &AppendEntriesReply{}
				for{
					ok := rf.sendAppendEntries(i, args, reply)
					DPrintf("raft %v send ae %v to %v,%v", rf.me, args,i,ok)
					mu.Lock()
					if ok && rf.role==Leader && rf.currentTerm==args.Term && reply.Result == true{
						AeCount += 1
						if( AeCount >= rf.half){							
							m:=logentryToapplymsg(&entry)
							rf.applyCh <- m
							rf.lastApplied++
							out,_ := json.Marshal(m)
							DPrintf("raft %v commit message %v,%v",rf.me, string(out),AeCount)
							AeCount=0
						}
						rf.nextIndex[i]=rf.commitIndex+1						
						break
					}
					mu.Unlock()
				}				
			}(i)
		}
	}
}
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader{
		return index, term, false
	}else{
		rf.commitIndex+=1
		index = rf.commitIndex
		term = rf.currentTerm
		rf.logs[rf.commitIndex]=LogEntry{command,index,term}
		go rf.SendCommandToAll(command, index,term)
		return index, term, isLeader 
	}	
}

func (rf *Raft) Run(){
	for{
		switch rf.role {
		case Follower:					
			x := time.Millisecond * time.Duration(rand.Intn(100) + 300)
			DPrintf("raft %v begin check heartbeat and selection,with role %v at %v, wait for %v",rf.me, rf.role, rf.currentTerm, x)	
			select{
			case <- rf.recHeart:
			case <- rf.recVote:
			case <- time.After(x):
				DPrintf("raft %v be %v  at %v not receive heartbeat after %v", rf.me,rf.role,rf.currentTerm, x)
				rf.role = Candidate
			}
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.voteCnt=1
			rf.voteFor=rf.me
			rf.mu.Unlock()
			x := time.Millisecond * time.Duration(rand.Intn(100) + 300)
			DPrintf("raft %v begin check heartbeat and selection,with role %v at %v,wait for %v",rf.me, rf.role, rf.currentTerm,x)			
			go	func(){
						for i := 0; i < rf.n; i++{
							if i != rf.me{
								go func(i int){
									req := &RequestVoteArgs{rf.currentTerm, rf.me,rf.logs[rf.commitIndex].Index,rf.logs[rf.commitIndex].Term}
									rep := &RequestVoteReply{}
									rf.sendRequestVote(i, req, rep)			
									DPrintf("raft %v %v at %v reqvote to %v res : %v ->%v",rf.me, rf.role,rf.currentTerm, i,rep, rf.voteCnt)			
									}(i)
							}
						}
					}()

			select{
			case <- rf.electWin:
				rf.role = Leader
				for i := range rf.nextIndex{
					rf.nextIndex[i]=rf.commitIndex+1
				}
				DPrintf("wow, raft %v become a leader at %v ", rf.me,rf.currentTerm)
			case <- rf.recHeart:
				rf.role = Follower
			case <- time.After(x):
				DPrintf("raft %v election timeout after for %v",rf.me,x)
			}
		case Leader:
			// for i := 0; i < rf.n; i++{
			// 	if i != rf.me{
			// 		// DPrintf("leader %v heart raft %v", rf.me, i)
			// 		go func(i int){		
			// 			args := &AppendEntriesArgs{
			// 				[]LogEntry{},
			// 				rf.currentTerm,
			// 				rf.me,
			// 				rf.logs[rf.commitIndex].Index,
			// 				rf.logs[rf.commitIndex].Term,
			// 				rf.lastApplied,
			// 			}
			// 			reply := &AppendEntriesReply{}
			// 			rf.sendAppendEntries(i, args, reply)
			// 		}(i)
			// 	}
			// }
			rf.SendHeartToAll()
			time.Sleep(120*time.Millisecond)
		}
	}
}
func (rf *Raft) SendHeartToI(server int) (*AppendEntriesReply, bool){
	entries := []LogEntry{}
	ni := rf.nextIndex[server]
	for i := ni; i <= rf.commitIndex;i++{
		entries = append(entries, rf.logs[i])
	}
	if server == (rf.me+1)%rf.n{
		log.Printf("raft %v send heart to %v ,error %v",rf.me, server, ni)
	}
	args := &AppendEntriesArgs{
		entries,
		rf.currentTerm,
		rf.me,
		rf.logs[ni-1].Index,
		rf.logs[ni-1].Term,
		rf.lastApplied,
	}

	reply := &AppendEntriesReply{}
	DPrintf("raft %v send ae %v to %v", rf.me, String(args),server)
	ok := rf.sendAppendEntries(server, args, reply)
	return reply,ok
}
func (rf *Raft) SendHeartToAll(){
	oldterm := rf.currentTerm
	for i := 0; i < rf.n; i++{
		if i != rf.me{
			// DPrintf("leader %v heart raft %v", rf.me, i)
			go func(i int){
				for{
					reply,ok := rf.SendHeartToI(i)
					if ok && rf.role==Leader && rf.currentTerm==oldterm{
						if reply.Result == true{
							break
						}else{
							rf.nextIndex[i] -= 1
						}
					}else{
						break
					}
				}				
			}(i)
		}
	}	
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
	rf.half = int(math.Ceil(float64(rf.n)/2))
	rf.currentTerm = 0
	rf.lastApplied=0
	rf.commitIndex=0
	rf.voteCnt = 0
	rf.voteFor = -1
	rf.applyCh = applyCh
	rf.recVote = make(chan string)
	rf.recHeart = make(chan string)	
	rf.electWin = make(chan string)
	rf.logs = make([]LogEntry,20)
	rf.logs[0]=LogEntry{-1,0,0}
	rf.nextIndex = make([]int,rf.n)
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	
	go rf.Run()
	return rf
}

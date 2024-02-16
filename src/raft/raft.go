package raft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// 当每个Raft节点意识到连续的日志条目被提交时，该节点应该通过传递给Make()的applyCh，
// 向同一服务器上的服务（或测试器）发送一个ApplyMsg。设置CommandValid为true以
// 表明ApplyMsg包含了一个新提交的日志条目。 在第2D部分，你会想通过applyCh发送其他类
// 型的消息（例如，快照），但对于这些其他用途，应将CommandValid设置为false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type RaftState string

const (
	Follower  RaftState = "Follower"
	Candidate           = "Candidate"
	Leader              = "Leader"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state         RaftState
	appendEntryCh chan *Entry
	heartBeat     time.Duration
	electionTime  time.Time

	// Persistent state on all servers:
	//所有服务器上的持久状态：
	currentTerm int
	votedFor    int
	log         Log

	// Volatile state on all servers:
	//所有服务器上的易失性状态：
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// 将Raft的持久状态保存到稳定存储中，这样在发生崩溃和重启后可以再次检索到。
// 参见论文中的图2，了解哪些内容应该是持久化的。
func (rf *Raft) persist() {
	DPrintVerbose("[%v]: STATE: %v", rf.me, rf.log.String())
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
// 恢复之前持久化的状态。
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs Log

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		log.Fatal("failed to read persist\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logs
	}
}

// 一个服务想要切换到快照。只有在Raft自从通过applyCh通报快照以来没有获取更多
// 最新信息的情况下才这样做。
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// 服务表明它已经创建了一个快照，包含了所有信息直到并包括某个索引。这意味着服务不再需要直到
// （并包括）那个索引的日志。Raft现在应该尽可能地裁剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// 使用Raft的服务（例如键/值服务器）希望开始就下一个将要追加到Raft日志中的命令达成一致。
// 如果此服务器不是领导者，则返回false。否则，开始协议并立即返回。 没有保证这个命令将被
// 提交到Raft日志中，因为领导者可能会失败或失去选举。即使Raft实例已被终止，这个函数也应该优雅地返回。
// 第一个返回值是如果这个命令被提交，它将出现在的索引位置。第二个返回值是当前的任期。第三个返回值是true，如果这个服务器相信它是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index := rf.log.lastLog().Index + 1
	term := rf.currentTerm

	log := Entry{
		Command: command,
		Index:   index,
		Term:    term,
	}
	rf.log.append(log)
	rf.persist()
	DPrintf("[%v]: term %v Start %v", rf.me, term, log)
	rf.appendEntries(false)

	return index, term, true
}

// 测试程序在每个测试之后不会停止Raft创建的goroutine，但它会调用Kill()方法。
// 你的代码可以使用killed()来检查是否已经调用了Kill()。使用原子操作避免了需要锁。
// 问题在于长时间运行的goroutine会使用内存并可能消耗CPU时间， 可能导致后续测
// 试失败并产生令人困惑的调试输出。任何带有长时间运行循环的goroutine 都应该调用
// killed()来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 如果这个节点最近没有收到心跳，则ticker协程会启动一次新的选举。
func (rf *Raft) ticker() {
	for rf.killed() == false {
		//检查是否应该启动一个领导者选举，并使用time.Sleep()来随机化睡眠时
		time.Sleep(rf.heartBeat)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.appendEntries(true)
		}
		if time.Now().After(rf.electionTime) {
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

// 这段描述提到了创建一个Raft服务器的过程和一些关键的组件及其用途：
//peers[]：这是一个包含所有Raft服务器端口的数组，包括正在创建的这一个服务器。
//peers[me]表示当前服务器的端口，其中me是当前服务器在数组中的索引。
//所有服务器中的peers[]数组都按相同的顺序排列，以确保每个服务器都可以通过相同
//的索引来引用其他服务器。 persister：这是当前服务器用来保存其持久状态的地方。
//持久状态是指即使在服务器崩溃和重启之后也需要保留的信息，比如日志条目、当前任
//期号以及已知的最高日志索引等。如果有的话，persister一开始也持有最近保存的状态。

//applyCh：这是一个通道，测试程序或服务通过它期望接收来自Raft的ApplyMsg消息。
//ApplyMsg消息是当Raft决定将日志条目应用到其状态机时发送的，这是分布式一致性算法的一部分，
//确保所有Raft节点上的状态机都能保持一致。

//Make()：这个函数用于初始化和创建一个Raft服务器实例。它必须快速返回，以避免阻塞调用者 。
//因此，任何长时间运行的工作，比如日志复制、选举等，都应该在goroutines中启动，
//以异步方式执行。这样做既可以快速启动服务器，又不会影响服务器响应其他Raft消息的能力。

// 总的来说，这段描述强调了在创建Raft服务器时需要考虑的几个关键因素，包括如何
// 管理服务器之间的通信、如何持久化状态以及如何处理长时间运行的任务。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.heartBeat = 50 * time.Millisecond
	rf.resetElectionTimer()

	rf.log = makeEmptyLog()
	rf.log.append(Entry{-1, 0, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	// 在崩溃前持久化的状态中初始化
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier()
	return rf
}

func (rf *Raft) apply() {
	rf.applyCond.Broadcast()
	DPrintf("[%v]: rf.applyCond.Broadcast()", rf.me)
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		// all server rule 1
		if rf.commitIndex > rf.lastApplied && rf.log.lastLog().Index > rf.lastApplied {
			rf.lastApplied++
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.at(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			DPrintVerbose("[%v]: COMMIT %d: %v", rf.me, rf.lastApplied, rf.commits())
			rf.mu.Unlock()
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
			DPrintf("[%v]: rf.applyCond.Wait()", rf.me)
		}
	}
}

func (rf *Raft) commits() string {
	nums := []string{}
	for i := 0; i <= rf.lastApplied; i++ {
		nums = append(nums, fmt.Sprintf("%4d", rf.log.at(i).Command))
	}
	return fmt.Sprint(strings.Join(nums, "|"))
}

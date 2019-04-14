# go test -run TestReElection2A >run.log
cut -d " " -f "3-" run.log|tee run.log2
# cut -d " " -f "3-" run.log.right.reElect |tee run.log2
cat run.log2|sed  '/raft 1/c\  '|sed  '/raft 2/c\  ' >raft0.log
cat run.log2|sed  '/raft 0/c\  '|sed  '/raft 2/c\  ' >raft1.log
cat run.log2|sed  '/raft 0/c\  '|sed  '/raft 1/c\  ' >raft2.log
paste raft0.log raft1.log raft2.log  >filtered.log
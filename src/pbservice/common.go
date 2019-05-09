package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	ErrReplica     = "ErrReplica"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	Xid   int64 // for checking idempotent
	Op    string //for indicating operation type
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}


// Your RPC definitions here.
type ForwardArgs struct {
	Key   string
	Value string
	Xid   int64
}

type ForwardReply struct {
	Err Err
}

type GetReplicaArgs struct {
	Srv string
}

type GetReplicaReply struct {
	Data map[string]string
	Dup  map[int64]Err
	Err  Err
}


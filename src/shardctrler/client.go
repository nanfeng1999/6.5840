package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	ClientId  int64
	RequestId int64
	LeaderId  int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.LeaderId = 0
	ck.ClientId = nrand()
	ck.RequestId = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num, args.ClientId, args.RequestId = num, ck.ClientId, ck.RequestId

	DPrintf("client {%d} send query = %v\n", ck.ClientId, args)
	for {
		var reply QueryReply
		if !ck.servers[ck.LeaderId].Call("ShardCtrler.Query", args, &reply) || reply.Err == TimeOut || reply.WrongLeader {
			//DPrintf("client {%d} get wrong reply = %v\n", ck.ClientId, reply)
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("client {%d} get right reply = %v\n", ck.ClientId, reply)
		ck.RequestId += 1
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers, args.ClientId, args.RequestId = servers, ck.ClientId, ck.RequestId

	DPrintf("client {%d} send join = %v\n", ck.ClientId, args)
	for {
		var reply JoinReply
		if !ck.servers[ck.LeaderId].Call("ShardCtrler.Join", args, &reply) || reply.Err == TimeOut || reply.WrongLeader {
			//DPrintf("client {%d} get wrong reply = %v\n", ck.ClientId, reply)
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("client {%d} get right reply = %v\n", ck.ClientId, reply)
		ck.RequestId += 1
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs, args.ClientId, args.RequestId = gids, ck.ClientId, ck.RequestId
	DPrintf("client {%d} send leave = %v\n", ck.ClientId, args)
	for {
		var reply LeaveReply
		if !ck.servers[ck.LeaderId].Call("ShardCtrler.Leave", args, &reply) || reply.Err == TimeOut || reply.WrongLeader {
			//DPrintf("client {%d} get wrong reply = %v\n", ck.ClientId, reply)
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("client {%d} get right reply = %v\n", ck.ClientId, reply)
		ck.RequestId += 1
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard, args.GID, args.ClientId, args.RequestId = shard, gid, ck.ClientId, ck.RequestId
	DPrintf("client {%d} send move = %v\n", ck.ClientId, args)
	for {
		var reply MoveReply
		if !ck.servers[ck.LeaderId].Call("ShardCtrler.Move", args, &reply) || reply.Err == TimeOut || reply.WrongLeader {
			//DPrintf("client {%d} get wrong reply = %v\n", ck.ClientId, reply)
			ck.LeaderId = (ck.LeaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("client {%d} get right reply = %v\n", ck.ClientId, reply)
		ck.RequestId += 1
		return
	}
}

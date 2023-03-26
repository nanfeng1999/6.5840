package shardctrler

import (
	"log"
	"math"
	"sort"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const ExecuteTimeout = 500 * time.Millisecond

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK      = "OK"
	TimeOut = "Timeout"
)

type OpType string

const (
	Join  = "Join"
	Move  = "Move"
	Leave = "Leave"
	Query = "Query"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	RequestId int64            // 客户端请求Id
	ClientId  int64            // 访问的客户端Id
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int
	RequestId int64 // 客户端请求Id
	ClientId  int64 // 访问的客户端Id
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int
	GID       int
	RequestId int64 // 客户端请求Id
	ClientId  int64 // 访问的客户端Id
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int   // desired config number
	RequestId int64 // 客户端请求Id
	ClientId  int64 // 访问的客户端Id
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type CommonReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type ReplyContext struct {
	LastRequestId int64
	Reply         CommonReply
}

func copyMap(oldMap map[int][]string) map[int][]string {
	newMap := make(map[int][]string, len(oldMap))
	for k, v := range oldMap {
		newSlice := make([]string, len(v))
		copy(newSlice, v)
		newMap[k] = newSlice
	}

	return newMap
}

func addMap(m1 map[int][]string, m2 map[int][]string) map[int][]string {
	for k2, v2 := range m2 {
		m1[k2] = v2
	}

	return m1
}

func rebalanceShards(shards [NShards]int, groups map[int][]string) [NShards]int {
	if len(groups) == 0 {
		return [NShards]int{}
	}

	shard2group := getShard2Group(shards)
	for gid, _ := range groups {
		if _, ok := shard2group[gid]; !ok {
			shard2group[gid] = make([]int, 0)
		}
	}

	DPrintf("shard2group = %v shards = %v groups = %v\n", shard2group, shards, groups)
	for {

		minGroup, maxGroup := getMinShardGroup(shard2group), getMaxShardGroup(shard2group)
		DPrintf("minGroup = %d maxGroup = %d\n", minGroup, maxGroup)
		if maxGroup != 0 && len(shard2group[maxGroup])-len(shard2group[minGroup]) <= 1 {
			DPrintf("shards = %v\n", shards)
			return shards
		}

		changeSid := shard2group[maxGroup][0]
		shards[changeSid] = minGroup
		shard2group[maxGroup] = shard2group[maxGroup][1:]
		shard2group[minGroup] = append(shard2group[minGroup], changeSid)

	}

}

func min(x int, y int) int {
	if x > y {
		return y
	}
	return x
}

func max(x int, y int) int {
	if x > y {
		return x
	}
	return y
}

func getMinShardGroup(shard2group map[int][]int) int {
	minNum, minGid := math.MaxInt, -1

	// 为了让map遍历具有确定性
	var keys []int
	for k := range shard2group {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, gid := range keys {
		if gid == 0 {
			continue
		}
		tmp := min(minNum, len(shard2group[gid]))
		if tmp < minNum {
			minNum, minGid = tmp, gid
		}
	}

	return minGid
}

func getMaxShardGroup(shard2group map[int][]int) int {
	maxNum, maxGid := math.MinInt, -1

	// 为了让map遍历具有确定性
	var keys []int
	for k := range shard2group {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for _, gid := range keys {
		if gid == 0 && len(shard2group[0]) > 0 {
			return 0
		}

		tmp := max(maxNum, len(shard2group[gid]))
		if tmp > maxNum {
			maxNum, maxGid = tmp, gid
		}
	}

	return maxGid
}

func getShard2Group(shards [NShards]int) map[int][]int {
	shard2group := make(map[int][]int)

	for sid, gid := range shards {
		if _, ok := shard2group[gid]; !ok {
			shard2group[gid] = make([]int, 0)
		}
		shard2group[gid] = append(shard2group[gid], sid)
	}

	return shard2group
}

func NewConfig() Config {
	return Config{
		Num:    0,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
}

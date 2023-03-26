package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
)

type CmdType string

type Command struct {
	Type CmdType
	Data interface{}
}

const (
	AddConfig   CmdType = "AddConfig"
	Operation           = "Operation"
	InsertShard         = "InsertShard"
	DeleteShard         = "DeleteShard"
)

// NewConfigCommand 添加新配置的命令
func NewConfigCommand(cfg shardctrler.Config) Command {
	return Command{Type: AddConfig, Data: cfg}
}

// NewOperationCommand 执行操作的命令Put、Append、Get
func NewOperationCommand(args OperationArgs) Command {
	return Command{Type: Operation, Data: args}
}

func NewInsertShardsCommand(reply GetShardReply) Command {
	return Command{Type: InsertShard, Data: reply}
}

func NewDeleteShardsCommand(args DeleteShardArgs) Command {
	return Command{Type: DeleteShard, Data: args}
}

func printShards(shards map[int]*Shard) string {
	tmp := ""
	for i, shard := range shards {
		tmp += fmt.Sprintf("%d %v\t", i, shard)
	}

	return tmp
}

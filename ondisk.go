package main

import (
	"fmt"
	"github.com/lni/dragonboat/v4"
	"os"
	"strconv"
	"strings"
	"time"
)

// 改变成员关系  -add addr replicaID && remove replicaID

func AppendMembers(mems map[uint64]string, mem_str string) (map[uint64]string, error) {
	added_mems := make(map[uint64]string, 10)
	//log.Printf("mem_str = %v", mem_str)
	mems_list := strings.Split(mem_str, "&")

	//log.Printf("mems_list:%v", mems_list)
	for _, mem := range mems_list {
		tmp_mem := strings.Split(mem, "_")
		id, _ := strconv.ParseUint(tmp_mem[0], 10, 64)
		addr := tmp_mem[1]
		added_mems[id] = addr
		added_mems[id] = addr
	}

	for _id, _mem := range mems {
		added_mems[_id] = _mem
	}
	return added_mems, nil

}

func makeMembershipChange(nh *dragonboat.NodeHost,
	cmd string, addr string, replicaID uint64) {
	var rs *dragonboat.RequestState
	var err error
	if cmd == "add" {
		// orderID is ignored in standalone mode
		rs, err = nh.RequestAddReplica(ExampleShardID, replicaID, addr, 0, 3*time.Second)
	} else if cmd == "remove" {
		rs, err = nh.RequestDeleteReplica(ExampleShardID, replicaID, 0, 3*time.Second)
	} else {
		panic("unknown cmd")
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "membership change failed, %v\n", err)
		return
	}
	select {
	case r := <-rs.CompletedC:
		if r.Completed() {
			fmt.Fprintf(os.Stdout, "membership change completed successfully\n")
		} else {
			fmt.Fprintf(os.Stderr, "membership change failed\n")
		}
	}
}

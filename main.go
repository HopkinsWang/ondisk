// Copyright 2017,2018 Lei Ni (nilei81@gmail.com).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
ondisk is an example program for dragonboat's on disk state machine.
*/
package main

import (
	"flag"
	"fmt"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/dragonboat/v4/config"
	"github.com/lni/dragonboat/v4/logger"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	//"github.com/HopkinsWang/ondisk/proto"
	//pb "RingAllReduce_29server/ondisk/proto"
)

type RequestType uint64

const (
	ExampleShardID uint64 = 128
)

const (
	PUT RequestType = iota
	GET
)

var (
	LocalhostCmd = "ifconfig eth0 | grep \"inet \" | awk -F \":\" '{print $1}' | awk '{print $2}'"
)

//type raftdServer struct {
//	pb.RaftdServiceServer
//	dockerAddr string
//	hostAddr string
//}

//var (
//	// initial nodes count is fixed to three, their addresses are also fixed
//	addresses = []string{
//		"localhost:63001",
//		"localhost:63002",
//		"localhost:63003",
//	}
//)

func ParseCommand(msg string) (RequestType, string, string, bool) {
	parts := strings.Split(strings.TrimSpace(msg), " ")
	if len(parts) == 0 || (parts[0] != "put" && parts[0] != "get") {
		return PUT, "", "", false
	}
	if parts[0] == "put" {
		if len(parts) != 3 {
			return PUT, "", "", false
		}
		return PUT, parts[1], parts[2], true
	}
	if len(parts) != 2 {
		return GET, "", "", false
	}
	return GET, parts[1], "", true
}

func printUsage() {
	fmt.Fprintf(os.Stdout, "Raft Usage - \n")
	fmt.Fprintf(os.Stdout, "put data into db:put key value\n")
	fmt.Fprintf(os.Stdout, "get data from db:get key\n")
}

func main() {
	replicaID := flag.Int("replicaid", 1, "ReplicaID to use")
	addr := flag.String("addr", "", "Nodehost address")
	join := flag.Bool("join", false, "Joining a new node")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap leader")
	flag.Parse()

	if runtime.GOOS == "darwin" {
		signal.Ignore(syscall.Signal(0xd))
	}
	initialMembers := make(map[uint64]string)
	if len(*addr) == 0 {
		panic("addr is null ondesk")
	}

	exe_res, exe_err, _err := ExecExternalScript(LocalhostCmd)
	log.Printf("exe_res=%v", exe_res)
	if _err != nil || exe_err != "" {
		log.Printf("exec_err: %v", _err)
	}

	old_addr := strings.Split(*addr, ":")
	newlocalAddr := fmt.Sprintf("%v:%v", exe_res, old_addr[1])

	log.Printf("oldAddr: %v:%v, newAddr: %v", old_addr[0], old_addr[1], newlocalAddr)
	if *bootstrap {
		initialMembers[1] = newlocalAddr
	}

	fmt.Fprintf(os.Stdout, "node address: %s\n", newlocalAddr)
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)
	rc := config.Config{
		ReplicaID:          uint64(*replicaID),
		ShardID:            ExampleShardID,
		ElectionRTT:        10,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}

	datadir := filepath.Join(
		"raft-cluster",
		"raft-data",
		fmt.Sprintf("node%d", *replicaID))
	nhc := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    newlocalAddr,
	}
	nh, err := dragonboat.NewNodeHost(nhc)
	if err != nil {
		panic(err)
	}

	if err := nh.StartOnDiskReplica(initialMembers, *join, NewDiskKV, rc); err != nil {
		fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
		os.Exit(1)
	}

	rs_port := 70000 + *replicaID

	raftserver := NewraftServer()
	raftserver.nh = nh
	raftserver.SelfAddr = *addr
	raftserver.SelfPort = rs_port
	//raftserver.Setdockeraddr(fmt.Sprintf("%v:%v",exe_res,rs_port))
	//raftserver.Sethostaddr(fmt.Sprintf("%v:%v",old_addr[0], rs_port))
	raftserver.Init()
	//consoleStopper := syncutil.NewStopper()
	printUsage()

	signalChan := make(chan os.Signal, 0)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	sig := <-signalChan
	log.Printf("%v", sig)
}

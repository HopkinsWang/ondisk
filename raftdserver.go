package main

import (
	"context"
	"encoding/json"
	"fmt"
	pb "ondisk/proto"
	//pb "github.com/HopkinsWang/ondisk/proto"
	"github.com/lni/dragonboat/v4"
	"github.com/lni/goutils/syncutil"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	Default_port = 61000
)

type raftServer struct {
	pb.RaftdServiceServer
	//dockerAddr string
	SelfAddr string
	SelfIp   string
	SelfPort int
	RaftID   int
	nh       *dragonboat.NodeHost

	//send_ch chan string
	//receive_get_ch chan string
	//receive_put_ch chan string
	//
	//cacheDB map[string]KVData

}

func NewraftServer() *raftServer {
	return &raftServer{SelfAddr: "0.0.0.0:61000", SelfIp: "0.0.0.0", SelfPort: Default_port, RaftID: 0}
}

func (rs *raftServer) SetSelfaddr(addr string) error {
	if IsvalidAddr(addr) {
		rs.SelfAddr = addr
		return nil
	}
	return nil
}

func (rs *raftServer) Getdockeraddr() string {
	return rs.SelfAddr
}

//func (rs *raftServer)Sethostaddr(addr string) error{
//	if IsvalidAddr(addr){
//		rs.hostAddr = addr
//		return nil
//	}
//	return errors.New(" addr is invalid!")
//}
//func (rs *raftServer) Gethostaddr() string{
//	return rs.hostAddr
//}

func IsvalidAddr(addr string) bool { //判断地址合法性
	if addr == "" {
		return false
	}
	if addr == "localhost" {
		return true
	}
	addr_list := strings.Split(addr, ":")
	if len(addr_list) == 2 { // ip + port
		port, err := strconv.ParseInt(addr_list[1], 10, 64)
		if err != nil {
			return false
		}
		if port > 2000 && port < 65535 { //2000默认是服务器不对外开放
			ip_net := strings.Split(addr_list[0], ".")
			if len(ip_net) == 4 {
				//valid_flag := true
				for _, net := range ip_net { //验证net字段合法性
					net_int, _err := strconv.ParseInt(net, 10, 64)
					if _err != nil || net_int < 0 || net_int > 255 {
						return false
					}
				}
				return true //只有IP 和 port 都合法才返回
			}
		}
	}
	return false
}

// put date in db of raft-cluster
func (rs *raftServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	raftStopper := syncutil.NewStopper()
	raftStopper.RunWorker(func() {
		cs := rs.nh.GetNoOPSession(ExampleShardID)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		kv := &KVData{
			Key: req.Key,
			Val: req.Val,
		}
		data, err := json.Marshal(kv)
		if err != nil {
			panic(err)
		}
		_, err = rs.nh.SyncPropose(ctx, cs, data)
		if err != nil {
			fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
		}
		cancel()
	})
	return &pb.PutResponse{Status: pb.Status_SUCCESS}, nil
}

func (rs *raftServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponde, error) {
	reponse := &pb.GetResponde{Key: req.Key}
	raftStopper := syncutil.NewStopper()
	raftStopper.RunWorker(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		result, err := rs.nh.SyncRead(ctx, ExampleShardID, []byte(req.Key))

		if err != nil {
			fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err)
			reponse.Val = ""
		} else {
			fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", req.Key, result)
			reponse.Val = fmt.Sprintf("%v", result)

		}
		cancel()
	})
	return reponse, nil
}

func (rs *raftServer) Init() error {
	//开启服务端
	rs.StartServer()
	return nil
}

func (rs *raftServer) StartServer() error {
	port := fmt.Sprintf("%v", rs.SelfPort)
	lis, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		log.Printf("Startserver Err: %v", err)
		log.Fatalf("failed to listen: %v", err)
	}

	size := 512 * 1024 * 1024
	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(size),
		grpc.MaxSendMsgSize(size),
	}
	grpcServer := grpc.NewServer(options...)
	pb.RegisterRaftdServiceServer(grpcServer, rs)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}

	return nil
}

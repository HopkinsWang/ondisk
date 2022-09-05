package main

import (
	// pb "RingAllReduce_29server/ondisk/proto"
	pb "github.com/HopkinsWang/ondisk/proto"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lni/dragonboat"
	"github.com/lni/goutils/syncutil"
	"os"
	"strconv"
	"strings"
	"time"
)

type raftServer struct {
	pb.RaftdServiceServer
	dockerAddr string
	hostAddr string
	nh *dragonboat.NodeHost

	send_ch chan string
	receive_get_ch chan string
	receive_put_ch chan string

	cacheDB map[string]KVData


}

func NewraftServer() *raftServer{
	return &raftServer{}
}

func (rs *raftServer)Setdockeraddr(addr string) error{
	if IsvalidAddr(addr){
		rs.dockerAddr = addr
		return nil
	}
	return errors.New(" addr is invalid!")
}

func (rs *raftServer) Getdockeraddr() string{
	return rs.dockerAddr
}

func (rs *raftServer)Sethostaddr(addr string) error{
	if IsvalidAddr(addr){
		rs.hostAddr = addr
		return nil
	}
	return errors.New(" addr is invalid!")
}
func (rs *raftServer) Gethostaddr() string{
	return rs.hostAddr
}

func IsvalidAddr(addr string) bool{    //判断地址合法性
	if addr ==""{
		return false
	}
	if addr =="localhost"{
		return true
	}
	addr_list := strings.Split(addr,":")
	if len(addr_list) ==2{           // ip + port
		port,err:= strconv.ParseInt(addr_list[1],10,64)
		if err!=nil{
			return false
		}
		if port>2000 && port <65535{   //2000默认是服务器不对外开放
			ip_net:=strings.Split(addr_list[0],".")
			if len(ip_net) ==4{
				//valid_flag := true
				for _ ,net := range ip_net{    //验证net字段合法性
					net_int,_err := strconv.ParseInt(net,10,64)
					if _err!=nil || net_int<0 ||net_int>255{
						return false
					}
				}
				return true     //只有IP 和 port 都合法才返回
			}
		}
	}
	return false
}


// put date in db of raft-cluster
func (rs *raftServer) Put(ctx context.Context,req *pb.PutRequest) (*pb.PutResponse,error){
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
	return &pb.PutResponse{Status: pb.Status_SUCCESS},nil
}

func (rs *raftServer) Get(ctx context.Context,req *pb.GetRequest)(*pb.GetResponde,error){
	reponse:= &pb.GetResponde{Key: req.Key}
	raftStopper := syncutil.NewStopper()
	raftStopper.RunWorker(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		result, err := rs.nh.SyncRead(ctx, ExampleShardID, []byte(req.Key))

		if err != nil {
			fmt.Fprintf(os.Stderr, "SyncRead returned error %v\n", err)
			reponse.Val =""
		} else {
			fmt.Fprintf(os.Stdout, "query key: %s, result: %s\n", req.Key, result)
			reponse.Val = fmt.Sprintf("%v",result)

		}
		cancel()
	})
	return reponse,nil
}

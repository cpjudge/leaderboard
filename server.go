package main

import (
	"flag"
	"io"
	"log"
	"net"

	pb "github.com/cpjudge/proto/leaderboard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/testdata"
)

var (
	tls        = flag.Bool("tls", false, "Connection uses TLS if true, else plain TCP")
	certFile   = flag.String("cert_file", "", "The TLS cert file")
	keyFile    = flag.String("key_file", "", "The TLS key file")
	jsonDBFile = flag.String("json_db_file", "", "A json file containing a list of features")
	serverAddr = flag.String("server_addr", "172.17.0.1:11000", "The server address in the format of host:port")
)

type leaderboardServer struct{}

func (s leaderboardServer) GetLeaderboard(srv pb.Leaderboard_GetLeaderboardServer) error {
	ctx := srv.Context()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		contest, err := srv.Recv()
		if err == io.EOF {
			// return will close stream from server side
			log.Println("exit")
			return nil
		}
		if err != nil {
			log.Printf("receive error %v", err)
			continue
		}
		if contest != nil {
			log.Println("Server received", contest.String())
			participants := processLeaderboard(contest.ContestId)
			if err := srv.Send(participants); err != nil {
				log.Printf("send error %v", err)
			}
		}
	}
}

func main() {
	flag.Parse()
	connectToDatabase()
	lis, err := net.Listen("tcp", *serverAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	if *tls {
		if *certFile == "" {
			*certFile = testdata.Path("server1.pem")
		}
		if *keyFile == "" {
			*keyFile = testdata.Path("server1.key")
		}
		creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterLeaderboardServer(grpcServer, &leaderboardServer{})
	grpcServer.Serve(lis)
}

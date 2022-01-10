package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/caetanofreitas/fc2-grpc/pb"
	"google.golang.org/grpc"
)

func AddUser(client pb.UserServiceClient) {
	fmt.Println("AddUser")
	req := &pb.User{
		Id:    "0",
		Name:  "Caetano",
		Email: "ca@gmail.com",
	}

	res, err := client.AddUser(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not request to gRPC Server: %v", err)
	}
	fmt.Println(res)
}

func AddUserVerbose(client pb.UserServiceClient) {
	fmt.Println("AddUserVerbose")
	req := &pb.User{
		Id:    "0",
		Name:  "Caetano 2",
		Email: "ca2@gmail.com",
	}

	responseStream, err := client.AddUserVerbose(context.Background(), req)
	if err != nil {
		log.Fatalf("Could not request to gRPC Server: %v", err)
	}

	for {
		stream, err := responseStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Could not recieve the message: %v", err)
		}

		fmt.Println("Status: ", stream.Status, " - ", stream.GetUser())
	}
}

func AddUsers(client pb.UserServiceClient) {
	fmt.Println("AddUsers")
	reqs := []*pb.User{
		&pb.User{
			Id:    "c1",
			Name:  "Caetano 1",
			Email: "cae1@ca.com",
		},
		&pb.User{
			Id:    "c2",
			Name:  "Caetano 2",
			Email: "cae2@ca.com",
		},
		&pb.User{
			Id:    "c3",
			Name:  "Caetano 3",
			Email: "cae3@ca.com",
		},
		&pb.User{
			Id:    "c4",
			Name:  "Caetano 4",
			Email: "cae4@ca.com",
		},
		&pb.User{
			Id:    "c5",
			Name:  "Caetano 5",
			Email: "cae5@ca.com",
		},
	}

	stream, err := client.AddUsers(context.Background())
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}
	for _, req := range reqs {
		stream.Send(req)
		time.Sleep(time.Second * 3)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error receiving response: %v", err)
	}

	fmt.Println(res)
}

func AddUserStreamBoth(client pb.UserServiceClient) {
	fmt.Println("AddUserStreamBoth")
	stream, err := client.AddUserStreamBoth(context.Background())
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	reqs := []*pb.User{
		&pb.User{
			Id:    "c1",
			Name:  "Caetano 1",
			Email: "cae1@ca.com",
		},
		&pb.User{
			Id:    "c2",
			Name:  "Caetano 2",
			Email: "cae2@ca.com",
		},
		&pb.User{
			Id:    "c3",
			Name:  "Caetano 3",
			Email: "cae3@ca.com",
		},
		&pb.User{
			Id:    "c4",
			Name:  "Caetano 4",
			Email: "cae4@ca.com",
		},
		&pb.User{
			Id:    "c5",
			Name:  "Caetano 5",
			Email: "cae5@ca.com",
		},
	}

	wait := make(chan int)

	go func() {
		for _, req := range reqs {
			fmt.Println("Sending user: ", req.Name)
			stream.Send(req)
			time.Sleep(time.Second * 2)
		}
		stream.CloseSend()
	}()

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error receiving data: %v", err)
				break
			}
			fmt.Printf("Receiving user %v with status: %v\n", res.User.GetName(), res.GetStatus())
		}
		close(wait)
	}()

	<-wait
}

func main() {
	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to gRPC Server: %v", err)
	}
	defer connection.Close()

	client := pb.NewUserServiceClient(connection)
	AddUser(client)

	AddUserVerbose(client)

	AddUsers(client)

	AddUserStreamBoth(client)
}

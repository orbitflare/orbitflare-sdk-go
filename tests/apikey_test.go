package tests

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	jetstreamv2 "github.com/orbitflare/orbitflare-sdk-go/jetstream/v2"
	pb "github.com/orbitflare/orbitflare-sdk-go/proto/jetstreamv2"
)

type versionServer struct {
	pb.UnimplementedJetstreamV2Server
	seen chan []string
}

func (s *versionServer) GetVersion(ctx context.Context, _ *pb.GetVersionRequest) (*pb.GetVersionResponse, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	s.seen <- md.Get("x-token")
	return &pb.GetVersionResponse{Version: "test"}, nil
}

func startVersionServer(t *testing.T) (string, chan []string) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	vs := &versionServer{seen: make(chan []string, 1)}
	pb.RegisterJetstreamV2Server(srv, vs)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return "http://" + lis.Addr().String(), vs.seen
}

func TestJetstreamV2SendsAPIKeyAsXToken(t *testing.T) {
	url, seen := startVersionServer(t)
	client, err := jetstreamv2.NewBuilder().URL(url).APIKey("ORBIT-123").Build()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.GetVersion(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := <-seen; len(got) != 1 || got[0] != "ORBIT-123" {
		t.Fatalf("x-token = %v", got)
	}
}

func TestJetstreamV2SendsNoXTokenWithoutKey(t *testing.T) {
	t.Setenv("ORBITFLARE_LICENSE_KEY", "")
	url, seen := startVersionServer(t)
	client, err := jetstreamv2.NewBuilder().URL(url).Build()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.GetVersion(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := <-seen; len(got) != 0 {
		t.Fatalf("expected no x-token, got %v", got)
	}
}

package httpgrpc

import (
	"github.com/CrowdStrike/csproto"
	"google.golang.org/grpc/encoding"
	_ "google.golang.org/grpc/encoding/proto"
)

func init() {
	encoding.RegisterCodec(csproto.GrpcCodec{})
}

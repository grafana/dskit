package httpgrpc

import (
	"strings"
	"testing"

	health "google.golang.org/grpc/health/grpc_health_v1"
)

var (
	smallVT = &HTTPRequest{
		Method: "GET",
		Url:    "/test",
		Body:   []byte("test"),
	}
	mediumVT = &HTTPRequest{
		Method: "GET",
		Url:    "/test",
		Body:   []byte(strings.Repeat(".", 2<<10)),
	}
	largeVT = &HTTPRequest{
		Method: "GET",
		Url:    "/test",
		Body:   []byte(strings.Repeat(".", 20<<10)),
	}

	smallVanilla  = &health.HealthCheckRequest{Service: string(smallVT.Body)}
	mediumVanilla = &health.HealthCheckRequest{Service: string(mediumVT.Body)}
	largeVanilla  = &health.HealthCheckRequest{Service: string(largeVT.Body)}

	//vtMsgs      = []*HTTPRequest{smallVT, mediumVT, largeVT}
	vanillaMsgs = []*health.HealthCheckRequest{smallVanilla, mediumVanilla, largeVanilla}
)

func BenchmarkCodec(b *testing.B) {
	smallVTBytes, err := smallVT.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	//var mediumVTBytes []byte
	mediumVTBytes, err := mediumVT.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	largeVTBytes, err := largeVT.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	vtMsgs := [][]byte{smallVTBytes, mediumVTBytes, largeVTBytes}

	names := []string{"sm", "md", "lg"}
	b.Run("marshall-unmarshall", func(b *testing.B) {
		//var err error
		//var bytes []byte
		for msgIdx, msgBytes := range vtMsgs {
			b.Run(names[msgIdx], func(b *testing.B) {
				for i := 0; i < b.N; i++ {

					outputMsg := new(HTTPRequest)
					err := outputMsg.Unmarshal(msgBytes)
					if err != nil {
						b.Fatal(err)
					}

					_, err = outputMsg.Marshal()
					if err != nil {
						b.Fatal(err)
					}

				}
			})
		}
	})

	//b.Run("marshall-unmarshall", func(b *testing.B) {
	//	//var err error
	//	//var bytes []byte
	//	for msgIdx, msgBytes := range vtMsgs {
	//		b.Run(names[msgIdx], func(b *testing.B) {
	//			for i := 0; i < b.N; i++ {
	//
	//				outputMsg := new(HTTPRequest)
	//				err := outputMsg.UnmarshalVT(msgBytes)
	//				if err != nil {
	//					b.Fatal(err)
	//				}
	//
	//				_, err = outputMsg.MarshalVT()
	//				if err != nil {
	//					b.Fatal(err)
	//				}
	//
	//			}
	//		})
	//	}
	//})
	//
	//b.Run("marshall-unmarshall", func(b *testing.B) {
	//	//var err error
	//	//var bytes []byte
	//	//var outputMsg *HTTPRequest
	//	for msgIdx, msgBytes := range vtMsgs {
	//		b.Run(names[msgIdx], func(b *testing.B) {
	//			for i := 0; i < b.N; i++ {
	//
	//				outputMsg := HTTPRequestFromVTPool()
	//				err := outputMsg.UnmarshalVT(msgBytes)
	//				if err != nil {
	//					b.Fatal(err)
	//				}
	//
	//				_, err = outputMsg.MarshalVT()
	//				if err != nil {
	//					b.Fatal(err)
	//				}
	//				outputMsg.ReturnToVTPool()
	//			}
	//		})
	//	}
	//})
}

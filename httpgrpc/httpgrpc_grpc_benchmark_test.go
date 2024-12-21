package httpgrpc

import (
	"strings"
	"testing"
)

var (
	small = &HTTPRequest{
		Method: "GET",
		Url:    "/test",
		Body:   []byte(strings.Repeat(".", 32)),
	}
	medium = &HTTPRequest{
		Method: "GET",
		Url:    "/test",
		Body:   []byte(strings.Repeat(".", 16*1024)),
	}
	large = &HTTPRequest{
		Method: "GET",
		Url:    "/test",
		Body:   []byte(strings.Repeat(".", 4*16*1024)),
	}
)

func BenchmarkCodec(b *testing.B) {
	smallBytes, err := small.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	mediumBytes, err := medium.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	largeBytes, err := large.Marshal()
	if err != nil {
		b.Fatal(err)
	}

	msgs := [][]byte{smallBytes, mediumBytes, largeBytes}

	names := []string{"sm", "md", "lg"}
	b.Run("marshall-unmarshall", func(b *testing.B) {
		for msgIdx, msgBytes := range msgs {
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

}

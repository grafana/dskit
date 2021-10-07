module github.com/grafana/dskit

go 1.16

require (
	github.com/armon/go-metrics v0.3.0
	github.com/coreos/etcd v3.3.25+incompatible // indirect
	github.com/go-kit/log v0.1.0
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang/snappy v0.0.4
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.1.0
	github.com/hashicorp/consul/api v1.9.1
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.2.3
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.26.0
	github.com/sercand/kuberesolver v2.4.0+incompatible // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	github.com/stretchr/testify v1.7.0
	github.com/weaveworks/common v0.0.0-20210913144402-035033b78a78
	go.etcd.io/etcd v3.3.25+incompatible
	go.etcd.io/etcd/api/v3 v3.5.0
	go.etcd.io/etcd/client/v3 v3.5.0
	go.uber.org/atomic v1.9.0
	golang.org/x/crypto v0.0.0-20210314154223-e6e6c4f2bb5b // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210611083556-38a9dc6acbc6
	google.golang.org/grpc v1.38.0
	gopkg.in/yaml.v2 v2.4.0
)

replace k8s.io/client-go v12.0.0+incompatible => k8s.io/client-go v0.21.4

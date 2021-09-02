module github.com/grafana/dskit

go 1.16

require (
	cloud.google.com/go v0.87.0 // indirect
	github.com/armon/go-metrics v0.3.6
	github.com/go-kit/kit v0.11.0
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang/snappy v0.0.4
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/consul/api v1.9.1
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-msgpack v0.5.5 // indirect
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.2.3
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.29.0
	github.com/prometheus/prometheus v1.8.2-0.20210720123808-b1ed4a0a663d
	github.com/sercand/kuberesolver v2.4.0+incompatible // indirect
	github.com/stretchr/testify v1.7.0
	github.com/weaveworks/common v0.0.0-20210722103813-e649eff5ab4a
	go.etcd.io/etcd v3.3.25+incompatible
	go.etcd.io/etcd/client/v3 v3.5.0
	go.etcd.io/etcd/server/v3 v3.5.0
	go.uber.org/atomic v1.9.0
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20210611083556-38a9dc6acbc6
	google.golang.org/grpc v1.39.0
	gopkg.in/yaml.v2 v2.4.0
)

replace k8s.io/client-go v12.0.0+incompatible => k8s.io/client-go v0.21.4

module github.com/grafana/dskit

go 1.16

require (
	cloud.google.com/go v0.87.0 // indirect
	github.com/HdrHistogram/hdrhistogram-go v1.1.0 // indirect
	github.com/armon/go-metrics v0.3.6
	github.com/go-kit/kit v0.11.0
	github.com/gogo/googleapis v1.4.0 // indirect
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.0
	github.com/golang/snappy v0.0.4
	github.com/gorilla/mux v1.8.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0
	github.com/hashicorp/consul/api v1.9.1
	github.com/hashicorp/go-cleanhttp v0.5.1
	github.com/hashicorp/go-sockaddr v1.0.2
	github.com/hashicorp/memberlist v0.2.3
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/miekg/dns v1.1.42 // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/common v0.26.0
	github.com/stretchr/testify v1.7.0
	github.com/thanos-io/thanos v0.19.1-0.20210816083900-2be2db775cbc
	github.com/weaveworks/common v0.0.0-20210419092856-009d1eebd624
	go.etcd.io/etcd v3.3.25+incompatible
	go.etcd.io/etcd/client/v3 v3.5.0
	go.etcd.io/etcd/server/v3 v3.5.0
	go.uber.org/atomic v1.9.0
	golang.org/x/time v0.0.0-20210220033141-f8bda1e9f3ba
	google.golang.org/grpc v1.39.0
	gopkg.in/yaml.v2 v2.4.0
)

replace k8s.io/client-go v12.0.0+incompatible => k8s.io/client-go v0.21.4

module github.com/grafana/dskit

go 1.25.8

toolchain go1.26.4

require (
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/cristalhq/hedgedhttp v0.9.1
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/facette/natsort v0.0.0-20181210072756-2cd4dd1e2dcb
	github.com/felixge/httpsnoop v1.1.0
	github.com/go-kit/log v0.2.1
	github.com/gogo/googleapis v1.4.1
	github.com/gogo/protobuf v1.3.2
	github.com/gogo/status v1.1.1
	github.com/golang/snappy v1.0.0
	github.com/google/go-cmp v0.7.0
	github.com/gorilla/mux v1.8.1
	github.com/grafana/gomemcache v0.0.0-20251127154401-74f93547077b
	github.com/grafana/otel-profiling-go v0.6.0
	github.com/grafana/pyroscope-go/godeltaprof v0.1.11
	github.com/hashicorp/consul/api v1.34.3
	github.com/hashicorp/go-cleanhttp v0.5.2
	github.com/hashicorp/go-metrics v0.6.0
	github.com/hashicorp/go-sockaddr v1.0.7
	github.com/hashicorp/golang-lru/v2 v2.0.7
	github.com/hashicorp/memberlist v0.5.3
	github.com/miekg/dns v1.1.72
	github.com/opentracing-contrib/go-grpc v0.1.4
	github.com/opentracing-contrib/go-stdlib v1.1.1
	github.com/opentracing/opentracing-go v1.2.0
	github.com/pires/go-proxyproto v0.12.0
	github.com/pkg/errors v0.9.1
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2
	github.com/prometheus/client_golang v1.23.2
	github.com/prometheus/client_model v0.6.2
	github.com/prometheus/common v0.69.0
	github.com/prometheus/exporter-toolkit v0.16.0
	github.com/sercand/kuberesolver/v6 v6.0.1
	github.com/stretchr/testify v1.11.1
	github.com/uber/jaeger-client-go v2.30.0+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible
	go.etcd.io/etcd/api/v3 v3.6.12
	go.etcd.io/etcd/client/pkg/v3 v3.6.12
	go.etcd.io/etcd/client/v3 v3.6.12
	go.opentelemetry.io/contrib/exporters/autoexport v0.69.0
	go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc v0.69.0
	go.opentelemetry.io/contrib/instrumentation/net/http/httptrace/otelhttptrace v0.69.0
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.69.0
	go.opentelemetry.io/contrib/propagators/jaeger v1.44.0
	go.opentelemetry.io/contrib/samplers/jaegerremote v0.37.1
	go.opentelemetry.io/otel v1.44.0
	go.opentelemetry.io/otel/exporters/jaeger v1.17.0
	go.opentelemetry.io/otel/sdk v1.44.0
	go.opentelemetry.io/otel/trace v1.44.0
	go.uber.org/atomic v1.11.0
	go.uber.org/goleak v1.3.0
	go.yaml.in/yaml/v3 v3.0.4
	golang.org/x/exp v0.0.0-20260611194520-c48552f49976
	golang.org/x/net v0.56.0
	golang.org/x/sync v0.21.0
	golang.org/x/time v0.15.0
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/HdrHistogram/hdrhistogram-go v1.2.0 // indirect
	github.com/armon/go-metrics v0.6.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.7.0 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/fsnotify/fsnotify v1.10.1 // indirect
	github.com/go-logfmt/logfmt v0.6.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-viper/mapstructure/v2 v2.5.0 // indirect
	github.com/golang-jwt/jwt/v5 v5.3.1 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/btree v1.1.3 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.29.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-hclog v1.6.3 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/go-msgpack/v2 v2.1.5 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-rootcerts v1.0.2 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/hashicorp/serf v0.10.2 // indirect
	github.com/jaegertracing/jaeger-idl v0.9.0 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/klauspost/compress v1.18.6 // indirect
	github.com/kylelemons/godebug v1.1.0 // indirect
	github.com/mattn/go-colorable v0.1.15 // indirect
	github.com/mattn/go-isatty v0.0.22 // indirect
	github.com/mdlayher/socket v0.6.1 // indirect
	github.com/mdlayher/vsock v1.3.0 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mwitkow/go-conntrack v0.0.0-20190716064945-2f068394615f // indirect
	github.com/prometheus/otlptranslator v1.0.0 // indirect
	github.com/prometheus/procfs v0.21.0 // indirect
	github.com/sean-/seed v0.0.0-20170313163322-e2103e2c3529 // indirect
	github.com/stretchr/objx v0.5.3 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/contrib/bridges/prometheus v0.69.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.66.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutlog v0.20.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdoutmetric v1.44.0 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.44.0 // indirect
	go.opentelemetry.io/otel/log v0.20.0 // indirect
	go.opentelemetry.io/otel/metric v1.44.0 // indirect
	go.opentelemetry.io/otel/sdk/log v0.20.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.44.0 // indirect
	go.opentelemetry.io/proto/otlp v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	go.yaml.in/yaml/v2 v2.4.4 // indirect
	golang.org/x/crypto v0.53.0 // indirect
	golang.org/x/mod v0.37.0 // indirect
	golang.org/x/oauth2 v0.36.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	golang.org/x/tools v0.47.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260622175928-b703f567277d // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260622175928-b703f567277d // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// Replace memberlist with our fork which includes some fixes that haven't been
// merged upstream yet.
replace github.com/hashicorp/memberlist => github.com/grafana/memberlist v0.3.1-0.20260515134459-1798cf41aca7

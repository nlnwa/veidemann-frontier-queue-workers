module github.com/nlnwa/veidemann-frontier-workers

go 1.16

require (
	github.com/go-redis/redis v6.15.9+incompatible // indirect
	github.com/nlnwa/veidemann-api/go v0.0.0-20211008092321-7fbcd3a6ae1a
	github.com/opentracing-contrib/go-grpc v0.0.0-20210225150812-73cb765af46e
	github.com/opentracing/opentracing-go v1.2.0
	github.com/prometheus/client_golang v1.11.0
	github.com/rs/zerolog v1.23.0
	github.com/spf13/pflag v1.0.5
	github.com/spf13/viper v1.8.1
	github.com/stretchr/testify v1.7.0
	github.com/uber/jaeger-client-go v2.29.1+incompatible
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/rethinkdb/rethinkdb-go.v6 v6.2.1
)

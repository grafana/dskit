package images

// If you change the image tag, remember to update it in the preloading done
// by GitHub actions (see .github/workflows/*).

// These are variables so that they can be modified.

var (
	Memcached = "memcached:1.6.1"
	Minio     = "minio/minio:RELEASE.2019-12-30T05-45-39Z"
	Consul    = "consul:1.8.4"
	ETCD      = "gcr.io/etcd-development/etcd:v3.4.7"
	DynamoDB  = "amazon/dynamodb-local:1.11.477"
)

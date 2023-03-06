package cache

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	dstls "github.com/grafana/dskit/crypto/tls"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/test"
)

var (
	defaultRedisClientConfig = RedisClientConfig{
		DialTimeout:            time.Second * 5,
		ReadTimeout:            time.Second * 3,
		WriteTimeout:           time.Second * 3,
		ConnectionPoolSize:     100,
		MinIdleConnections:     10,
		IdleTimeout:            time.Minute * 5,
		MaxGetMultiConcurrency: 100,
		MaxGetMultiBatchSize:   100,
		MaxItemSize:            16 * 1024 * 1024,
		MaxAsyncConcurrency:    50,
		MaxAsyncBufferSize:     25000,
		TLSEnabled:             false,
		TLS:                    dstls.ClientConfig{},
	}
)

func TestRedisClient(t *testing.T) {
	// Init some data to conveniently define test cases later one.
	key1 := "key1"
	key2 := "key2"
	key3 := "key3"
	value1 := []byte{1}
	value2 := []byte{2}
	value3 := []byte{3}

	type args struct {
		data      map[string][]byte
		fetchKeys []string
	}
	type want struct {
		hits map[string][]byte
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			name: "all hit",
			args: args{
				data: map[string][]byte{
					key1: value1,
					key2: value2,
					key3: value3,
				},
				fetchKeys: []string{key1, key2, key3},
			},
			want: want{
				hits: map[string][]byte{
					key1: value1,
					key2: value2,
					key3: value3,
				},
			},
		},
		{
			name: "partial hit",
			args: args{
				data: map[string][]byte{
					key1: value1,
					key2: value2,
				},
				fetchKeys: []string{key1, key2, key3},
			},
			want: want{
				hits: map[string][]byte{
					key1: value1,
					key2: value2,
				},
			},
		},
		{
			name: "not hit",
			args: args{
				data:      map[string][]byte{},
				fetchKeys: []string{key1, key2, key3},
			},
			want: want{
				hits: map[string][]byte{},
			},
		},
	}
	s, err := miniredis.Run()
	require.NoError(t, err)

	defer s.Close()
	redisConfigs := []struct {
		name        string
		redisConfig func() RedisClientConfig
	}{
		{
			name: "MaxConcurrency and MaxGetMultiBatchSize set to a value > 0",
			redisConfig: func() RedisClientConfig {
				cfg := defaultRedisClientConfig
				cfg.Endpoint = flagext.StringSliceCSV{s.Addr()}
				cfg.MaxGetMultiConcurrency = 2
				cfg.MaxGetMultiBatchSize = 2
				return cfg
			},
		},
		{
			name: "MaxConcurrency and MaxGetMultiBatchSize set to 0",
			redisConfig: func() RedisClientConfig {
				cfg := defaultRedisClientConfig
				cfg.Endpoint = flagext.StringSliceCSV{s.Addr()}
				cfg.MaxGetMultiConcurrency = 0
				cfg.MaxGetMultiBatchSize = 0
				return cfg
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, redisConfig := range redisConfigs {
				t.Run(tt.name+redisConfig.name, func(t *testing.T) {
					logger := log.NewLogfmtLogger(os.Stderr)
					reg := prometheus.NewRegistry()

					c, err := NewRedisClient(logger, t.Name(), redisConfig.redisConfig(), reg)
					require.NoError(t, err)

					defer c.Stop()
					defer s.FlushAll()
					for k, v := range tt.args.data {
						_ = c.SetAsync(k, v, time.Hour)
					}

					test.Poll(t, time.Second, true, func() interface{} {
						hits := c.GetMulti(context.Background(), tt.args.fetchKeys)
						return reflect.DeepEqual(tt.want.hits, hits)
					})
				})
			}
		})
	}
}

func TestRedisClientDelete(t *testing.T) {
	s, err := miniredis.Run()
	require.NoError(t, err)
	defer s.Close()

	cfg := defaultRedisClientConfig
	cfg.Endpoint = flagext.StringSliceCSV{s.Addr()}

	c, err := NewRedisClient(log.NewLogfmtLogger(os.Stderr), "test-delete", cfg, prometheus.NewRegistry())
	require.NoError(t, err)

	defer c.Stop()
	defer s.FlushAll()

	key1 := "key1"
	value1 := []byte{1}

	_ = c.SetAsync(key1, value1, time.Hour)

	test.Poll(t, time.Second, true, func() interface{} {
		hits := c.GetMulti(context.Background(), []string{key1})
		return reflect.DeepEqual(map[string][]byte{key1: value1}, hits)
	})

	// Delete key1
	_ = c.Delete(context.Background(), key1)

	test.Poll(t, time.Second, true, func() interface{} {
		hits := c.GetMulti(context.Background(), []string{key1})
		return reflect.DeepEqual(map[string][]byte{}, hits)
	})
}

func TestValidateRedisConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    func() RedisClientConfig
		expectErr bool // func(*testing.T, interface{}, error)
	}{
		{
			name: "simpleConfig",
			config: func() RedisClientConfig {
				cfg := defaultRedisClientConfig
				cfg.Endpoint = flagext.StringSliceCSV{"127.0.0.1:6789"}
				cfg.Username = "user"
				cfg.Password = flagext.SecretWithValue("1234")
				return cfg
			},
			expectErr: false,
		},
		{
			name: "tlsConfigDefaults",
			config: func() RedisClientConfig {
				cfg := defaultRedisClientConfig
				cfg.Endpoint = flagext.StringSliceCSV{"127.0.0.1:6789"}
				cfg.Username = "user"
				cfg.Password = flagext.SecretWithValue("1234")
				cfg.TLSEnabled = true
				return cfg
			},
			expectErr: false,
		},
		{
			name: "tlsClientCertConfig",
			config: func() RedisClientConfig {
				cfg := defaultRedisClientConfig
				cfg.Endpoint = flagext.StringSliceCSV{"127.0.0.1:6789"}
				cfg.Username = "user"
				cfg.Password = flagext.SecretWithValue("1234")
				cfg.TLSEnabled = true
				cfg.TLS = dstls.ClientConfig{
					CertPath: "cert/client.pem",
					KeyPath:  "cert/client.key",
				}
				return cfg
			},
			expectErr: true,
		},
		{
			name: "tlsInvalidClientCertConfig",
			config: func() RedisClientConfig {
				cfg := defaultRedisClientConfig
				cfg.Endpoint = flagext.StringSliceCSV{"127.0.0.1:6789"}
				cfg.Username = "user"
				cfg.Password = flagext.SecretWithValue("1234")
				cfg.TLSEnabled = true
				cfg.TLS = dstls.ClientConfig{
					CertPath: "cert/client.pem",
				}
				return cfg
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.config()

			logger := log.NewLogfmtLogger(os.Stderr)
			reg := prometheus.NewRegistry()
			val, err := NewRedisClient(logger, tt.name, cfg, reg)
			if val != nil {
				defer val.Stop()
			}

			if tt.expectErr {
				require.Nil(t, val)
				require.Error(t, err)
			} else {
				require.NotNil(t, val)
			}
		})
	}
}

package consul

import (
	"context"

	consul "github.com/hashicorp/consul/api"
	"github.com/weaveworks/common/instrument"
)

type consulMetrics struct {
	kv
	consulRequestDuration *instrument.HistogramCollector
}

func (c consulMetrics) CAS(p *consul.KVPair, options *consul.WriteOptions) (bool, *consul.WriteMeta, error) {
	var ok bool
	var result *consul.WriteMeta
	err := instrument.CollectedRequest(options.Context(), "CAS", c.consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		ok, result, err = c.kv.CAS(p, options)
		return err
	})
	return ok, result, err
}

func (c consulMetrics) Get(key string, options *consul.QueryOptions) (*consul.KVPair, *consul.QueryMeta, error) {
	var kvp *consul.KVPair
	var meta *consul.QueryMeta
	err := instrument.CollectedRequest(options.Context(), "Get", c.consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		kvp, meta, err = c.kv.Get(key, options)
		return err
	})
	return kvp, meta, err
}

func (c consulMetrics) List(path string, options *consul.QueryOptions) (consul.KVPairs, *consul.QueryMeta, error) {
	var kvps consul.KVPairs
	var meta *consul.QueryMeta
	err := instrument.CollectedRequest(options.Context(), "List", c.consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		kvps, meta, err = c.kv.List(path, options)
		return err
	})
	return kvps, meta, err
}

func (c consulMetrics) Delete(key string, options *consul.WriteOptions) (*consul.WriteMeta, error) {
	var meta *consul.WriteMeta
	err := instrument.CollectedRequest(options.Context(), "Delete", c.consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		meta, err = c.kv.Delete(key, options)
		return err
	})
	return meta, err
}

func (c consulMetrics) Put(p *consul.KVPair, options *consul.WriteOptions) (*consul.WriteMeta, error) {
	var result *consul.WriteMeta
	err := instrument.CollectedRequest(options.Context(), "Put", c.consulRequestDuration, instrument.ErrorCode, func(ctx context.Context) error {
		options = options.WithContext(ctx)
		var err error
		result, err = c.kv.Put(p, options)
		return err
	})
	return result, err
}

package cache

import (
	"context"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
)

type (
	Cache interface {
		Set(ctx context.Context, key string, value []byte) error
		SetNX(ctx context.Context, key string, value interface{}, exp time.Duration) (bool, error)
		SetAnyExp(ctx context.Context, key string, value interface{}, exp time.Duration) error
		SetExp(ctx context.Context, key string, value []byte, exp time.Duration) error
		Get(ctx context.Context, key string, object interface{}) error
		GetBytes(ctx context.Context, key string) ([]byte, error)
		MGet(ctx context.Context, keys []string, object interface{}) error
		Del(ctx context.Context, keys ...string) error
		Incr(ctx context.Context, key string) error
		Decr(ctx context.Context, key string) error
		Keys(ctx context.Context, pattern string) ([]string, error)
		Ping(ctx context.Context) error
		Close() error
		GetHashed() (CacheHashed, error)
	}

	CacheHashed interface {
		HSet(ctx context.Context, key string, field string, val interface{}) error
		HExist(ctx context.Context, key string, field string) (bool, error)
		HGet(ctx context.Context, key string, field string, val interface{}) error
	}

	Option struct {
		Addresses []string
		// deprecated: use Addresses instead. When Addresses only one will use standalone redis else will use redis cluster
		Address            string
		UserName, Password string
		// DB only used when using redis standalone
		DB                                                 int
		PoolSize, MinIdleConn                              int
		DialTimeout, ReadTimeout, WriteTimeout, MaxConnAge time.Duration
	}

	cch struct {
		cache redis.UniversalClient
	}
)

func (c *cch) HSet(ctx context.Context, key string, field string, val interface{}) error {
	return c.cache.HSet(ctx, key, field, val).Err()
}

func (c *cch) HExist(ctx context.Context, key string, field string) (bool, error) {
	return c.cache.HExists(ctx, key, field).Result()
}

func (c *cch) HGet(ctx context.Context, key string, field string, val interface{}) error {
	return c.cache.HGet(ctx, key, field).Scan(val)
}

func (c *cch) GetHashed() (CacheHashed, error) {
	return c, nil
}

func (c *cch) Set(ctx context.Context, key string, value []byte) error {
	return c.SetExp(ctx, key, value, 0)
}

func (c *cch) SetExp(ctx context.Context, key string, value []byte, exp time.Duration) error {
	var (
		status = c.cache.Set(ctx, key, value, exp)
	)
	return status.Err()
}

func (c *cch) SetAnyExp(ctx context.Context, key string, value interface{}, exp time.Duration) error {
	var (
		status = c.cache.Set(ctx, key, value, exp)
	)
	return status.Err()
}

func (c *cch) SetNX(ctx context.Context, key string, value interface{}, exp time.Duration) (bool, error) {
	var (
		status = c.cache.SetNX(ctx, key, value, exp)
	)
	return status.Result()
}

func (c *cch) Get(ctx context.Context, key string, object interface{}) error {
	var (
		status = c.cache.Get(ctx, key)
	)

	if err := status.Err(); err != nil {
		return err
	}

	return status.Scan(object)
}

func (c *cch) GetBytes(ctx context.Context, key string) ([]byte, error) {
	var (
		status = c.cache.Get(ctx, key)
	)

	if err := status.Err(); err != nil {
		return nil, err
	}

	return status.Bytes()
}

func (c *cch) MGet(ctx context.Context, keys []string, object interface{}) error {
	var (
		status = c.cache.MGet(ctx, keys...)
	)

	if err := status.Err(); err != nil {
		return err
	}

	return status.Scan(object)
}

func (c *cch) Del(ctx context.Context, keys ...string) error {
	var (
		err = c.cache.Del(ctx, keys...).Err()
	)

	return err
}

func (c *cch) Incr(ctx context.Context, key string) error {
	return c.cache.Incr(ctx, key).Err()
}

func (c *cch) Decr(ctx context.Context, key string) error {
	return c.cache.Decr(ctx, key).Err()
}

func (c *cch) Keys(ctx context.Context, pattern string) ([]string, error) {
	var (
		res = c.cache.Keys(ctx, pattern)
	)

	return res.Result()
}

func (c *cch) Ping(ctx context.Context) error {
	return c.cache.Ping(ctx).Err()
}

func (c *cch) Close() error {
	return c.cache.Close()
}

func New(option *Option) (Cache, error) {
	if len(option.Addresses) == 1 {
		client := redis.NewClient(&redis.Options{
			Addr:         option.Addresses[0],
			Username:     option.UserName,
			Password:     option.Password,
			DB:           option.DB,
			DialTimeout:  option.DialTimeout,
			ReadTimeout:  option.ReadTimeout,
			WriteTimeout: option.WriteTimeout,
			MaxConnAge:   option.MaxConnAge,
			PoolSize:     option.PoolSize,
			MinIdleConns: option.MinIdleConn,
		})
		return &cch{client}, nil
	}

	if option.DB > 0 {
		return nil, errors.New("unsupported db option on cluster redis")
	}

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        option.Addresses,
		Username:     option.UserName,
		Password:     option.Password,
		DialTimeout:  option.DialTimeout,
		ReadTimeout:  option.ReadTimeout,
		WriteTimeout: option.WriteTimeout,
		MaxConnAge:   option.MaxConnAge,
		PoolSize:     option.PoolSize,
		MinIdleConns: option.MinIdleConn,
	})

	return &cch{client}, nil
}

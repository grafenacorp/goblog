package cache

import (
	redisotel "github.com/go-redis/redis/extra/redisotel/v8"
)

// TODO: maybe we could upgrade redis lib version to v9 ?

func UsePluginOpenTelemetry(cache Cache) error {
	switch cacheType := cache.(type) {
	case *cch:
		cacheType.cache.AddHook(redisotel.NewTracingHook())
		return nil
	default:
		return nil
	}
}

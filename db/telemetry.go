package db

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/plugin/opentelemetry/tracing"
)

func WithoutMetrics() tracing.Option        { return tracing.WithoutMetrics() }
func WithoutQueryVariables() tracing.Option { return tracing.WithoutQueryVariables() }
func WithAttributes(attrs ...attribute.KeyValue) tracing.Option {
	return tracing.WithAttributes(attrs...)
}
func WithDBName(dbname string) tracing.Option { return tracing.WithDBName(dbname) }
func WithQueryFormatter(queryFormatter func(query string) string) tracing.Option {
	return tracing.WithQueryFormatter(queryFormatter)
}
func WithTracerProvider(provider trace.TracerProvider) tracing.Option {
	return tracing.WithTracerProvider(provider)
}

func UsePluginTelemetry(orm ORM, opts ...tracing.Option) error {

	switch ormType := orm.(type) {
	case *mysqldb:
		return ormType.db.Use(tracing.NewPlugin(opts...))
	default:
		return nil
	}
}

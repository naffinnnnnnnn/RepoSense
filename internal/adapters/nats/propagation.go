package nats

import (
	"context"

	gonats "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type natsHeaderCarrier struct{ header gonats.Header }

func (c natsHeaderCarrier) Get(key string) string { return c.header.Get(key) }
func (c natsHeaderCarrier) Set(key, value string) { c.header.Set(key, value) }
func (c natsHeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c.header))
	for key := range c.header {
		keys = append(keys, key)
	}
	return keys
}

func injectTraceContext(ctx context.Context, header gonats.Header) {
	otel.GetTextMapPropagator().Inject(ctx, natsHeaderCarrier{header: header})
}

func extractTraceContext(ctx context.Context, header gonats.Header) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, propagation.TextMapCarrier(natsHeaderCarrier{header: header}))
}

package graphapp

import (
	"context"

	"github.com/reposense/reposense/internal/ports"
)

type graphContextObserver interface {
	StartStage(context.Context, string, map[string]string) (context.Context, func(error))
}

func startGraphStage(observer ports.Observer, ctx context.Context, name string, attributes map[string]string) (context.Context, func(error)) {
	if contextual, ok := observer.(graphContextObserver); ok {
		return contextual.StartStage(ctx, name, attributes)
	}
	return ctx, observer.Stage(ctx, name, attributes)
}

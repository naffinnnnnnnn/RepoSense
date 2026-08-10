package observability

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"
)

type Observer struct {
	logger   *log.Logger
	mu       sync.Mutex
	counters map[string]int64
}

func New(logger *log.Logger) *Observer {
	if logger == nil {
		logger = log.Default()
	}
	return &Observer{logger: logger, counters: map[string]int64{}}
}
func (o *Observer) Stage(ctx context.Context, name string, attrs map[string]string) func(error) {
	started := time.Now()
	return func(err error) {
		fields := map[string]any{"message": "仓库解析阶段", "stage": name, "duration_ms": time.Since(started).Milliseconds()}
		for k, v := range attrs {
			fields[k] = v
		}
		if err != nil {
			fields["level"], fields["status"], fields["error_type"] = "ERROR", "error", errorType(err)
		} else {
			fields["level"], fields["status"] = "INFO", "ok"
		}
		encoded, _ := json.Marshal(fields)
		o.logger.Print(string(encoded))
	}
}
func (o *Observer) Count(name string, value int64, attrs map[string]string) {
	o.mu.Lock()
	o.counters[name] += value
	total := o.counters[name]
	o.mu.Unlock()
	fields := map[string]any{"level": "DEBUG", "message": "仓库解析器指标", "metric": name, "value": value, "total": total}
	for k, v := range attrs {
		fields[k] = v
	}
	encoded, _ := json.Marshal(fields)
	o.logger.Print(string(encoded))
}
func errorType(err error) string {
	if err == nil {
		return ""
	}
	return "operation_failed"
}

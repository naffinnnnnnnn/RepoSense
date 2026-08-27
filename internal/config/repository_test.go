package config

import "testing"

func TestRepositoryRuntimeRejectsMissingProductionDependenciesAndInvalidLimits(t *testing.T) {
	t.Setenv("REPOSENSE_POSTGRES_DSN", "")
	t.Setenv("REPOSENSE_NATS_URL", "")
	if _, err := LoadRepositoryRuntime(); err == nil {
		t.Fatal("生产 Store 和 Publisher 配置缺失时必须启动失败")
	}
	t.Setenv("REPOSENSE_POSTGRES_DSN", "postgres://example")
	t.Setenv("REPOSENSE_NATS_URL", "nats://example")
	t.Setenv("REPOSENSE_MAX_FILES", "0")
	if _, err := LoadRepositoryRuntime(); err == nil {
		t.Fatal("非正数资源配置必须启动失败")
	}
}

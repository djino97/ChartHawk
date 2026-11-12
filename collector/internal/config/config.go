// Package config загружает конфигурацию Go-сервиса сбора данных (Collector)
// из config.yaml. Здесь же хранится адрес WebSocket/gRPC-эндпоинта T-Invest
// API — он вынесен в общие настройки сервиса, а не захардкожен в клиенте.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config — корневая конфигурация Collector-а.
type Config struct {
	Tinkoff       TinkoffConfig       `yaml:"tinkoff"`
	HTTP          HTTPConfig          `yaml:"http"`
	Kafka         KafkaConfig         `yaml:"kafka"`
	Subscriptions SubscriptionsConfig `yaml:"subscriptions"`
	Log           LogConfig           `yaml:"log"`
}

// TinkoffConfig — общие настройки подключения к T-Invest API.
// EndPoint задаётся здесь централизованно, чтобы его можно было менять
// между боевым и sandbox-контурами без правок кода клиента.
type TinkoffConfig struct {
	// EndPoint — адрес стрим-сервиса T-Invest API (host:port для gRPC).
	// Пример: invest-public-api.tinkoff.ru:443
	EndPoint string `yaml:"endpoint"`
	// Token — токен доступа, читается из переменной окружения через ${VAR}.
	Token string `yaml:"token"`
	// AppName — идентификатор приложения, передаётся в x-app-name.
	AppName string `yaml:"app_name"`
	// AccountID — брокерский счёт. Для сбора рыночных данных не нужен.
	// Пустое значение на sandbox-адресе оставляет автооткрытие счёта в SDK;
	// на боевом адресе клиент sandbox не вызывает.
	AccountID string `yaml:"account_id"`
	// MaxRetries — количество ретраев unary-запросов на уровне SDK.
	MaxRetries uint `yaml:"max_retries"`

	// Reconnect — параметры экспоненциального backoff для переподключения
	// стрима рыночных данных (см. 2.5 в аналитике).
	Reconnect ReconnectConfig `yaml:"reconnect"`
}

// ReconnectConfig — параметры backoff с jitter для WS/gRPC-стрима.
type ReconnectConfig struct {
	InitialInterval     time.Duration `yaml:"initial_interval"`
	MaxInterval         time.Duration `yaml:"max_interval"`
	Multiplier          float64       `yaml:"multiplier"`
	RandomizationFactor float64       `yaml:"randomization_factor"`
}

// HTTPConfig — настройки REST API управления подписками (2.7).
type HTTPConfig struct {
	Addr        string `yaml:"addr"`
	AdminAPIKey string `yaml:"admin_api_key"`
}

// KafkaConfig — настройки продюсера Kafka (2.6).
type KafkaConfig struct {
	Brokers      []string      `yaml:"brokers"`
	CandlesTopic string        `yaml:"candles_topic"`
	BatchSize    int           `yaml:"batch_size"`
	BatchTimeout time.Duration `yaml:"batch_timeout"`
}

// SubscriptionGroup — группа инструментов на один таймфрейм.
type SubscriptionGroup struct {
	Timeframe   string   `yaml:"timeframe"`
	Instruments []string `yaml:"instruments"`
}

// SubscriptionsConfig — начальный список подписок и лимиты (2.4, 2.7).
type SubscriptionsConfig struct {
	Initial      []SubscriptionGroup `yaml:"initial"`
	MaxPerStream int                 `yaml:"max_per_stream"`
	MaxStreams   int                 `yaml:"max_streams"`
}

// LogConfig — настройки логирования.
type LogConfig struct {
	Level string `yaml:"level"`
}

func setDefaults(c *Config) {
	if c.Tinkoff.EndPoint == "" {
		c.Tinkoff.EndPoint = "invest-public-api.tinkoff.ru:443"
	}
	if c.Tinkoff.AppName == "" {
		c.Tinkoff.AppName = "charthawk-collector"
	}
	if c.Tinkoff.MaxRetries == 0 {
		c.Tinkoff.MaxRetries = 3
	}
	if c.Tinkoff.Reconnect.InitialInterval == 0 {
		c.Tinkoff.Reconnect.InitialInterval = time.Second
	}
	if c.Tinkoff.Reconnect.MaxInterval == 0 {
		c.Tinkoff.Reconnect.MaxInterval = 30 * time.Second
	}
	if c.Tinkoff.Reconnect.Multiplier == 0 {
		c.Tinkoff.Reconnect.Multiplier = 1.5
	}
	if c.Tinkoff.Reconnect.RandomizationFactor == 0 {
		c.Tinkoff.Reconnect.RandomizationFactor = 0.15
	}
	if c.HTTP.Addr == "" {
		c.HTTP.Addr = ":8081"
	}
	if len(c.Kafka.Brokers) == 0 {
		c.Kafka.Brokers = []string{"localhost:9092"}
	}
	if c.Kafka.CandlesTopic == "" {
		c.Kafka.CandlesTopic = "market.candles"
	}
	if c.Kafka.BatchSize == 0 {
		c.Kafka.BatchSize = 100
	}
	if c.Kafka.BatchTimeout == 0 {
		c.Kafka.BatchTimeout = 100 * time.Millisecond
	}
	if c.Subscriptions.MaxPerStream == 0 {
		c.Subscriptions.MaxPerStream = 300
	}
	if c.Subscriptions.MaxStreams == 0 {
		c.Subscriptions.MaxStreams = 1
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
}

func validate(c *Config) error {
	if c.Tinkoff.Token == "" {
		return fmt.Errorf("tinkoff.token is required (set TINKOFF_TOKEN env var)")
	}
	if c.HTTP.AdminAPIKey == "" {
		return fmt.Errorf("http.admin_api_key is required (set ADMIN_API_KEY env var)")
	}
	return nil
}

// Load читает и парсит config.yaml, раскрывая ${VAR}-подстановки из
// окружения перед разбором YAML.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	expanded := os.ExpandEnv(string(raw))

	var c Config
	if err := yaml.Unmarshal([]byte(expanded), &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	setDefaults(&c)
	if err := validate(&c); err != nil {
		return nil, err
	}

	return &c, nil
}

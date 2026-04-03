package config

import (
	"os"

	"sigs.k8s.io/yaml"
)

type Config struct {
	LogLevel            int    `yaml:"logLevel"`
	MetricsPort         int    `yaml:"metricsPort"`
	MetricsEndpointAuth bool   `yaml:"metricsEndpointAuth"`
	Concurrency         int    `yaml:"concurrency"`
	RequestMergePolicy  string `yaml:"requestMergePolicy"`
	MessageQueueImpl    string `yaml:"messageQueueImpl"`
	PrometheusURL       string `yaml:"prometheusURL"`

	Redis          RedisConfig          `yaml:"redis"`
	RedisSortedSet RedisSortedSetConfig `yaml:"redisSortedSet"`
	PubSub         PubSubConfig         `yaml:"pubSub"`
	Gates          GatesConfig          `yaml:"gates"`
}

type RedisConnConfig struct {
	Addr     string `yaml:"addr"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type RedisConfig struct {
	Conn               RedisConnConfig `yaml:"conn"`
	IGWBaseURL         string          `yaml:"igwBaseURL"`
	RequestPathURL     string          `yaml:"requestPathURL"`
	InferenceObjective string          `yaml:"inferenceObjective"`
	RequestQueueName   string          `yaml:"requestQueueName"`
	RetryQueueName     string          `yaml:"retryQueueName"`
	ResultQueueName    string          `yaml:"resultQueueName"`
	QueuesConfigFile   string          `yaml:"queuesConfigFile"`
}

type RedisSortedSetConfig struct {
	Conn               RedisConnConfig `yaml:"conn"`
	IGWBaseURL         string          `yaml:"igwBaseURL"`
	RequestPathURL     string          `yaml:"requestPathURL"`
	InferenceObjective string          `yaml:"inferenceObjective"`
	RequestQueueName   string          `yaml:"requestQueueName"`
	ResultQueueName    string          `yaml:"resultQueueName"`
	QueuesConfigFile   string          `yaml:"queuesConfigFile"`
	PollIntervalMs     int             `yaml:"pollIntervalMs"`
	BatchSize          int             `yaml:"batchSize"`
	GateType           string          `yaml:"gateType"`
	GateParamsJSON     string          `yaml:"gateParams"`
}

type PubSubConfig struct {
	IGWBaseURL          string `yaml:"igwBaseURL"`
	ProjectID           string `yaml:"projectID"`
	RequestPathURL      string `yaml:"requestPathURL"`
	InferenceObjective  string `yaml:"inferenceObjective"`
	RequestSubscriberID string `yaml:"requestSubscriberID"`
	ResultTopicID       string `yaml:"resultTopicID"`
	TopicsConfigFile    string `yaml:"topicsConfigFile"`
	BatchSize           int    `yaml:"batchSize"`
}

type GatesConfig struct {
	Saturation SaturationGateConfig `yaml:"saturation"`
	Prometheus PrometheusGateConfig `yaml:"prometheus"`
}

type SaturationGateConfig struct {
	InferencePool string  `yaml:"inferencePool"`
	Threshold     float64 `yaml:"threshold"`
	Fallback      float64 `yaml:"fallback"`
	QueryExpr     string  `yaml:"queryExpr"`
}

type PrometheusGateConfig struct {
	IsGMP          bool   `yaml:"isGMP"`
	URL            string `yaml:"url"`
	GMPProjectID   string `yaml:"gmpProjectID"`
	QueryModelName string `yaml:"queryModelName"`
}

func DefaultConfig() *Config {
	return &Config{
		LogLevel:            0, // logging.DEFAULT is usually 0
		MetricsPort:         9090,
		MetricsEndpointAuth: true,
		Concurrency:         8,
		RequestMergePolicy:  "random-robin",
		MessageQueueImpl:    "redis-pubsub",

		Redis: RedisConfig{
			Conn: RedisConnConfig{
				Addr:     "localhost:6379",
				User:     os.Getenv("REDIS_USERNAME"),
				Password: os.Getenv("REDIS_PASSWORD"),
			},
			RequestPathURL:   "/v1/completions",
			RequestQueueName: "request-queue",
			RetryQueueName:   "retry-sortedset",
			ResultQueueName:  "result-queue",
		},
		RedisSortedSet: RedisSortedSetConfig{
			Conn: RedisConnConfig{
				Addr:     "localhost:6379",
				User:     os.Getenv("REDIS_USERNAME"),
				Password: os.Getenv("REDIS_PASSWORD"),
			},
			RequestPathURL:   "/v1/completions",
			RequestQueueName: "request-sortedset",
			ResultQueueName:  "result-list",
			PollIntervalMs:   1000,
			BatchSize:        10,
			GateParamsJSON:   "{}",
		},
		PubSub: PubSubConfig{
			RequestPathURL: "/v1/completions",
			BatchSize:      10,
		},
		Gates: GatesConfig{
			Saturation: SaturationGateConfig{
				Threshold: 0.8,
				Fallback:  0.0,
			},
			Prometheus: PrometheusGateConfig{
				IsGMP: false,
			},
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()
	if path == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

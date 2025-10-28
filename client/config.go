package client

import (
	"time"

	"go.temporal.io/sdk/client"
)

// Config holds the configuration for Temporal client
type Config struct {
	// Connection settings
	HostPort  string
	Namespace string

	// Authentication settings
	Identity string
	TLS      *TLSConfig
	Auth     *AuthConfig

	// Connection options
	ConnectionTimeout time.Duration
	QueryTimeout      time.Duration
	WorkerCount       int

	// Retry settings
	RetryMaxAttempts     int
	RetryInitialInterval time.Duration
	RetryMaxInterval     time.Duration

	// Interceptors
	Interceptors []interface{}

	// Metrics settings
	MetricsHandler interface{}

	// Logger
	Logger interface{}
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	CertPath   string
	KeyPath    string
	CACertPath string
	ServerName string
	Insecure   bool
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	APIKey      string
	Token       string
	TokenPath   string
	Certificate string
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		HostPort:             "localhost:7233",
		Namespace:            "default",
		ConnectionTimeout:    10 * time.Second,
		QueryTimeout:         10 * time.Second,
		WorkerCount:          1,
		RetryMaxAttempts:     3,
		RetryInitialInterval: 1 * time.Second,
		RetryMaxInterval:     30 * time.Second,
	}
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.HostPort == "" {
		return ErrInvalidHostPort
	}
	if c.Namespace == "" {
		return ErrInvalidNamespace
	}
	return nil
}

// ToClientOptions converts Config to Temporal client options
func (c *Config) ToClientOptions() client.Options {
	opts := client.Options{
		HostPort:  c.HostPort,
		Namespace: c.Namespace,
	}

	if c.Identity != "" {
		opts.Identity = c.Identity
	}

	if c.ConnectionTimeout > 0 {
		// Note: DialTimeout is not available in this SDK version
		// Can be configured through connection options if needed
	}

	// Note: Optional configurations removed for compatibility
	// These can be added when the specific SDK version is available:
	// - MetricsHandler
	// - Logger
	// - Interceptors

	return opts
}

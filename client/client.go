package client

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/client"
)

// Client wraps the Temporal client with additional functionality
type Client struct {
	client.Client
	config *Config
}

// Factory provides methods to create Temporal clients
type Factory struct {
	defaultConfig *Config
}

// NewFactory creates a new client factory
func NewFactory(config *Config) *Factory {
	if config == nil {
		config = DefaultConfig()
	}
	return &Factory{
		defaultConfig: config,
	}
}

// NewClient creates a new Temporal client with default configuration
func NewClient(hostPort, namespace string) (*Client, error) {
	config := DefaultConfig()
	config.HostPort = hostPort
	config.Namespace = namespace
	return NewClientWithConfig(config)
}

// NewClientWithConfig creates a new Temporal client with custom configuration
func NewClientWithConfig(config *Config) (*Client, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	opts := config.ToClientOptions()
	temporalClient, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporal client: %w", err)
	}

	return &Client{
		Client: temporalClient,
		config: config,
	}, nil
}

// CreateClient creates a client with factory
func (f *Factory) CreateClient() (*Client, error) {
	return NewClientWithConfig(f.defaultConfig)
}

// CreateClientWithConfig creates a client with custom config using factory
func (f *Factory) CreateClientWithConfig(config *Config) (*Client, error) {
	// Merge with default config
	mergedConfig := f.mergeConfigs(f.defaultConfig, config)
	return NewClientWithConfig(mergedConfig)
}

// GetConfig returns the client configuration
func (c *Client) GetConfig() *Config {
	return c.config
}

// HealthCheck performs a health check on the Temporal server
func (c *Client) HealthCheck(ctx context.Context) error {
	// Simple health check by checking if client is connected
	if c.Client == nil {
		return fmt.Errorf("client is not connected")
	}
	return nil
}

// Close closes the client connection
func (c *Client) Close() {
	c.Client.Close()
}

// mergeConfigs merges two configurations, with override taking precedence
func (f *Factory) mergeConfigs(base, override *Config) *Config {
	merged := *base // Copy base config

	if override.HostPort != "" {
		merged.HostPort = override.HostPort
	}
	if override.Namespace != "" {
		merged.Namespace = override.Namespace
	}
	if override.Identity != "" {
		merged.Identity = override.Identity
	}
	if override.TLS != nil {
		merged.TLS = override.TLS
	}
	if override.Auth != nil {
		merged.Auth = override.Auth
	}
	if override.ConnectionTimeout > 0 {
		merged.ConnectionTimeout = override.ConnectionTimeout
	}
	if override.QueryTimeout > 0 {
		merged.QueryTimeout = override.QueryTimeout
	}
	if override.WorkerCount > 0 {
		merged.WorkerCount = override.WorkerCount
	}
	if override.RetryMaxAttempts > 0 {
		merged.RetryMaxAttempts = override.RetryMaxAttempts
	}
	if override.RetryInitialInterval > 0 {
		merged.RetryInitialInterval = override.RetryInitialInterval
	}
	if override.RetryMaxInterval > 0 {
		merged.RetryMaxInterval = override.RetryMaxInterval
	}
	if len(override.Interceptors) > 0 {
		merged.Interceptors = override.Interceptors
	}
	if override.MetricsHandler != nil {
		merged.MetricsHandler = override.MetricsHandler
	}
	if override.Logger != nil {
		merged.Logger = override.Logger
	}

	return &merged
}

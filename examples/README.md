# Examples Directory

This directory contains comprehensive examples demonstrating all features of the `go-mod-temporal` module.

## Available Examples

### 1. Cron Jobs (`examples/cron/`)
Demonstrates various cron job patterns:
- **Daily cron jobs** - Execute at specific times daily
- **Hourly cron jobs** - Execute at specific minutes each hour  
- **Weekly cron jobs** - Execute on specific days and times
- **Custom cron expressions** - Use any valid cron expression
- **Resilient cron jobs** - Continue execution despite failures
- **Manual triggering** - Trigger cron jobs on demand
- **Status monitoring** - Check cron job status and history

**Features shown:**
✅ Cron job looping setiap waktu
✅ WorkflowID generation otomatis
✅ Manual trigger untuk testing
✅ Status monitoring dan tracking

### 2. One-Shot Executions (`examples/oneshot/`)
Demonstrates single execution patterns:
- **Immediate execution** - Execute workflows right away
- **Scheduled execution** - Execute at specific future times
- **Delayed execution** - Execute after a custom delay
- **Resilient execution** - Execute with retry policies
- **Batch execution** - Execute multiple tasks with staggered timing
- **Cancellation** - Cancel scheduled executions

**Features shown:**
✅ Sekali eksekusi (one-shot execution)
✅ Eksekusi pada waktu yang ditentukan
✅ Delay execution dengan custom timing
✅ Batch processing dengan staggered execution

### 3. Retry Patterns (`examples/retry/`)
Demonstrates comprehensive retry strategies:
- **Basic retry** - Retry until success or timeout
- **Limited retry** - Stop after maximum attempts
- **Exponential backoff** - Increase delay between retries
- **Circuit breaker** - Stop after consecutive failures
- **Custom retry** - Configurable retry policies
- **Scheduled retry** - Retry at specific times
- **Conditional retry** - Retry only on specific error types
- **Batch retry** - Parallel retry operations

**Features shown:**
✅ Retry eksekusi apabila gagal (by parameter)
✅ Multiple retry strategies
✅ Failure handling policies
✅ Circuit breaker untuk prevent cascade failures

### 4. Complete Integration (`examples/complete/`)
Demonstrates all features working together:
- **Multi-worker architecture** - Different workers for different tasks
- **Comprehensive scheduling** - All scheduling types combined
- **Policy-based execution** - Different policies for different scenarios
- **Dynamic management** - Runtime schedule and worker management
- **Monitoring and metrics** - Full observability setup
- **Failure scenarios** - Testing and recovery patterns

**Features shown:**
✅ Semua fitur temporal terintegrasi
✅ Production-ready architecture
✅ Comprehensive monitoring
✅ All user requirements combined

## Quick Start

### Prerequisites
1. **Temporal Server**: Start using Docker
   ```bash
   docker run --rm -p 7233:7233 -p 8233:8233 temporalio/auto-setup:latest
   ```

2. **Go Module**: Ensure you're in the module directory
   ```bash
   cd /path/to/go-temporal-module
   ```

### Running Examples

1. **Cron Jobs Example:**
   ```bash
   go run examples/cron/main.go
   ```

2. **One-Shot Example:**
   ```bash
   go run examples/oneshot/main.go
   ```

3. **Retry Patterns Example:**
   ```bash
   go run examples/retry/main.go
   ```

4. **Complete Integration Example:**
   ```bash
   go run examples/complete/main.go
   ```

## Monitoring

All examples can be monitored through:
- **Temporal Web UI**: http://localhost:8233
- **Console logs**: Structured logging with execution details
- **Metrics**: Prometheus metrics (when configured)

## Example Output

Each example provides detailed console output showing:
- Worker startup and registration
- Schedule creation and IDs
- Execution results and timing
- Error handling and retry attempts
- Status monitoring and health checks

## Customization

All examples can be customized by:
- Modifying workflow parameters
- Changing schedule expressions
- Adjusting retry policies
- Adding custom failure scenarios
- Integrating with external services

## Error Scenarios

Examples include intentional failures to demonstrate:
- Retry mechanisms
- Failure policies
- Circuit breaker patterns
- Recovery strategies
- Error monitoring

## Production Usage

The complete example (`examples/complete/`) shows production-ready patterns:
- Proper worker lifecycle management
- Comprehensive error handling
- Monitoring and observability
- Resource optimization
- Security considerations

## Next Steps

After running the examples:
1. Explore the source code to understand implementation details
2. Modify examples to match your specific use cases
3. Integrate patterns into your applications
4. Set up monitoring and alerting
5. Configure production-ready policies

For detailed API documentation, see the main README.md file.
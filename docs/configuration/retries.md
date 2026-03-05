# Resiliency Policies

Polecat includes built-in resiliency for transient SQL Server failures using [Polly](https://github.com/App-vNext/Polly).

## Default Behavior

By default, Polecat retries on transient SQL Server errors (deadlocks, timeouts, connection failures) with an exponential backoff strategy.

## Custom Polly Configuration

### Replace the Default Pipeline

```cs
opts.ConfigurePolly(builder =>
{
    builder.AddRetry(new RetryStrategyOptions
    {
        MaxRetryAttempts = 5,
        BackoffType = DelayBackoffType.Exponential,
        Delay = TimeSpan.FromMilliseconds(200)
    });
});
```

### Extend the Default Pipeline

```cs
opts.ExtendPolly(builder =>
{
    builder.AddTimeout(TimeSpan.FromSeconds(30));
});
```

## Circuit Breaker

You can add a circuit breaker to prevent cascading failures:

```cs
opts.ExtendPolly(builder =>
{
    builder.AddCircuitBreaker(new CircuitBreakerStrategyOptions
    {
        FailureRatio = 0.5,
        MinimumThroughput = 10,
        SamplingDuration = TimeSpan.FromSeconds(30),
        BreakDuration = TimeSpan.FromSeconds(15)
    });
});
```

# Gunicorn Timeout Fix

## Problem

LLM calls (especially schema discovery) take longer than 30 seconds, causing worker timeouts:
```
[CRITICAL] WORKER TIMEOUT (pid:69)
Worker (pid:69) was sent SIGKILL!
```

## Solution

Increased gunicorn timeout to 300 seconds (5 minutes) to allow LLM operations to complete.

## Configuration

### Procfile
```
web: gunicorn --bind 0.0.0.0:$PORT --timeout 300 --graceful-timeout 300 --workers 2 web_dashboard:app
```

### Parameters Explained

- `--timeout 300`: Maximum time (in seconds) a worker can take to process a request (5 minutes)
- `--graceful-timeout 300`: Time to wait for workers to finish after receiving restart signal
- `--workers 2`: Number of worker processes (2 for better concurrency)

## Why 300 Seconds?

- LLM API calls can take 10-60 seconds
- Schema discovery is a complex operation that may take 30-120 seconds
- Query translation with multiple steps can take 60-180 seconds
- 300 seconds (5 minutes) provides a safe buffer

## Alternative: Async Workers

If timeouts persist, consider using async workers:

```
web: gunicorn --bind 0.0.0.0:$PORT --timeout 300 --worker-class gevent --workers 2 web_dashboard:app
```

This requires adding `gevent` to requirements.txt.

## Monitoring

Check Render logs for:
- Worker timeout messages
- Request processing times
- LLM call durations

If you see timeouts even with 300s, consider:
1. Optimizing LLM prompts (shorter, more focused)
2. Using async/background tasks for long operations
3. Implementing request queuing
4. Using faster LLM models for simple operations

## Current Status

✅ Timeout increased to 300 seconds
✅ Configuration pushed to GitHub
✅ Render will auto-redeploy with new settings


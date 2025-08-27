# Temporal Batching Demo

**Problem**: Multiple concurrent workflows writing to a database can overwhelm the target system.

**Solution**: Use a dedicated batcher workflow to aggregate writes from many workflows into efficient bulk operations.

## How It Works

- **Batch Aggregation**: Central batcher collects write requests from concurrent workflows
- **Time-Based Batching**: Writes batched every 20 seconds (configurable)
- **Acknowledgment**: Each workflow receives confirmation when write completes
- **Load Reduction**: N individual DB calls → 1 batch operation
- **Exactly-Once Processing**: Request deduplication prevents duplicate writes
- **Continue-as-New**: Automatic workflow restart to prevent unbounded event history growth

## Architecture

**Main Service** (`main-service/`):
- Handles transactional business logic workflows
- Queue: `main-workflow-queue`

**Batcher Service** (`batcher-service/`):
- Aggregates and batches write operations  
- Queue: `batcher-queue`

## File Structure

```
temporal-batching-demo/
├── README.md
├── pyproject.toml
├── uv.lock
│
├── main-service/
│   ├── workflow.py          # Transactional main workflow with timeout handling
│   ├── activities.py        # Business activities
│   ├── worker.py           # Main service worker
│   └── starter.py          # Start main workflows
│
└── batcher-service/
    ├── workflow.py          # Batcher workflow with continue-as-new
    ├── activities.py        # Batch write activities
    ├── worker.py           # Batcher worker
    └── starter.py          # Start batcher service with health monitoring
```

## Quick Start

1. **Install dependencies:**
   ```bash
   uv sync
   ```

2. **Start Temporal:**
   ```bash
   temporal server start-dev
   ```

3. **Run workers (separate terminals):**
   ```bash
   uv run python batcher-service/worker.py
   uv run python main-service/worker.py
   ```

4. **Start services:**
   ```bash
   # Terminal 3
   uv run python batcher-service/starter.py
   
   # Terminal 4
   uv run python main-service/starter.py --workflows 10
   ```

5. **View results:**
   - Console: See batch write operations and health monitoring
   - Web UI: http://localhost:8233

## What You'll See

1. Main workflows start and process business logic
2. Each signals the batcher with write requests (with request IDs for deduplication)
3. Batcher collects requests for 20 seconds or until 100 requests
4. Single batch write operation executes (printed to console)
5. Batcher confirms completion to all workflows
6. Main workflows complete successfully, or fail if write not confirmed within 2 minutes
7. Batcher continues-as-new every 1000 signals to prevent event history growth

## Key Features

### **Transactional Integrity**
- Main workflows fail if write request cannot be submitted
- Main workflows fail if write confirmation not received within 2 minutes
- Main workflows fail if database write reports failure

### **Exactly-Once Processing**
- Request deduplication using deterministic request IDs
- Duplicate signals are safely ignored
- Deduplication state survives continue-as-new

### **Continue-as-New**
- Batcher restarts every 1000 signals to prevent unbounded event history
- State (pending writes, counters, processed request IDs) preserved across restarts
- Health monitoring detects and handles continue-as-new transitions

### **Error Handling**
- Comprehensive retry logic for signal delivery failures
- Individual confirmation failures don't affect other workflows
- Proper exception handling causes workflow failures rather than endless retries

### **Monitoring**
The starter script provides real-time monitoring:
- Pending write requests
- Processed batch count
- Total signals received
- Continue-as-new events
- Payload size estimation

## Production Considerations

### **Current Limitations**
- **Continue-as-New Payload Size**: High signal volumes can create large state payloads during continue-as-new
- **Single Batcher**: One batcher workflow processes all requests
- **Memory-Based Deduplication**: Processed request IDs stored in workflow memory
- **Unbounded State Growth**: Without proper cleanup, processed request IDs and pending writes can grow indefinitely, causing continue-as-new failures when payloads exceed Temporal's limits

### **Scaling Solutions**
- **Shard Batchers**: Multiple batcher workflows with deterministic routing

## Recommended Thresholds
- **Batch Size**: 100 requests (configurable)
- **Batch Timeout**: 20 seconds (configurable)
- **Continue-as-New**: 1000 signals (prevents large payloads)
- **Write Confirmation Timeout**: 2 minutes (ensures transactional integrity)
- **Deduplication Window**: Last 500 processed request IDs

Perfect for medium-scale use cases (thousands of requests per minute). For higher volumes, implement sharding or external deduplication storage.
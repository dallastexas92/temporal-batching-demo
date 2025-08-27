# Temporal Batching Demo

**Problem**: Multiple concurrent workflows writing to a database can overwhelm the target system.

**Solution**: Use a dedicated batcher workflow to aggregate writes from many workflows into efficient bulk operations.

## How It Works

- **Batch Aggregation**: Central batcher collects write requests from concurrent workflows
- **Time-Based Batching**: Writes batched every 20 seconds (configurable)
- **Acknowledgment**: Each workflow receives confirmation when write completes
- **Load Reduction**: N individual DB calls → 1 batch operation

## Architecture

**Main Service** (`main-service/`):
- Handles business logic workflows
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
│   ├── workflow.py          # Main business workflow
│   ├── activities.py        # Business activities
│   ├── worker.py           # Main service worker
│   └── starter.py          # Start main workflows
│
└── batcher-service/
    ├── workflow.py          # Batcher workflow
    ├── activities.py        # Batch write activities
    ├── worker.py           # Batcher worker
    └── starter.py          # Start batcher service
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
   python batcher-service/worker.py
   python main-service/worker.py
   ```

4. **Start services:**
   ```bash
   # Terminal 3
   python batcher-service/starter.py
   
   # Terminal 4
   python main-service/starter.py --workflows 10
   ```

5. **View results:**
   - Console: See batch write operations
   - Web UI: http://localhost:8233

## What You'll See

1. Main workflows start and process business logic
2. Each signals the batcher with write requests
3. Batcher collects requests for 20 seconds
4. Single batch write operation executes (printed to console)
5. Batcher confirms completion to all workflows
6. Workflows finish processing

## Production Considerations

**Limitations:**
- Signal limits: ~10k per batcher workflow
- Event history grows with each signal
- Single batcher = bottleneck

**Solutions:**
- **Shard batchers**: Multiple batcher workflows
- **Continue-as-new**: Reset event history periodically  
- **External queues**: Use Redis/SQS for unlimited scale
- **Activity-based**: Move batching logic to activities

Perfect for medium-scale use cases. High volume needs sharding or external queuing.
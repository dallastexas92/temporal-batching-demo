# Temporal Batching Demo

A simple demonstration of batching database writes using Temporal workflows in Python.

## True Microservice Architecture

Each service is completely independent:

**Main Service:**
- Manages its own workflows via `start_main_workflows.py`
- Runs its own worker via `main_worker.py`
- Uses its own queue: `main-workflow-queue`

**Batcher Service:**
- Manages its own workflow via `start_batcher.py`  
- Runs its own worker via `batcher_worker.py`
- Uses its own queue: `batcher-queue`

## File Structure

```
batching-demo/
├── start_main_workflows.py   # CLI to start main workflows
├── start_batcher.py          # CLI to start batcher
├── main.py                   # [DEPRECATED - use separate scripts]
├── pyproject.toml            # Dependencies
├── README.md                 # This file
│
├── main_workflow.py          # Main business workflow
├── main_activities.py        # Activities for main workflow  
├── main_worker.py           # Worker for main workflows
│
├── batcher_workflow.py       # Batching workflow
├── batcher_activities.py     # Activities for batcher
└── batcher_worker.py        # Worker for batcher workflow
```

## Setup

1. **Install dependencies:**
   ```bash
   uv sync
   ```

2. **Start Temporal server:**
   ```bash
   # Using Temporal CLI (recommended)
   temporal server start-dev

   # Or using Docker
   docker run -p 7233:7233 -p 8233:8233 temporalio/auto-setup:latest
   ```

3. **Start the workers (in separate terminals):**
   ```bash
   # Terminal 1: Batcher worker
   python batcher_worker.py
   
   # Terminal 2: Main workflow worker  
   python main_worker.py
   ```

4. **Start the batcher service (in a third terminal):**
   ```bash
   python start_batcher.py
   ```

5. **Start main workflows (in a fourth terminal):**
   ```bash
   # Start 5 workflows (default)
   python start_main_workflows.py

   # Start 10 workflows
   python start_main_workflows.py --workflows 10

   # Start 50 workflows (test batching!)
   python start_main_workflows.py --workflows 50
   ```

## What You'll See

1. Multiple main workflows start and do initial work
2. Each signals the batcher with their data
3. Batcher collects requests for 20 seconds (or until 100 requests)
4. Batcher performs a batch write (printed to console)
5. Batcher signals completion back to each workflow  
6. Main workflows receive confirmation and complete

## Temporal Web UI

View running workflows at: http://localhost:8233

## Key Features

- **Deterministic workflows**: All non-deterministic operations in activities
- **Proper error handling**: Retry policies and timeouts
- **Signal-based coordination**: Cross-workflow communication
- **Batch optimization**: Reduces database load from N calls to 1 call per batch

## Next Steps

For production scale, consider:
- **Sharding**: Multiple batcher workflows to avoid signal limits
- **External queues**: Redis/SQS for even higher throughput  
- **Dead letter queues**: Handle failed batch writes
- **Metrics**: Monitor batch sizes and timing
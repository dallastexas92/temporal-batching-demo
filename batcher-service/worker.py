import asyncio
from temporalio.client import Client
from temporalio.worker import Worker

from workflow import BatcherWorkflow
from activities import batch_write_to_database


async def run_batcher_worker():
    """Start the worker for Batcher workflow"""
    client = await Client.connect("localhost:7233")
    
    worker = Worker(
        client,
        task_queue="batcher-queue",
        workflows=[BatcherWorkflow],
        activities=[batch_write_to_database],
    )
    
    print("🚀 Batcher Worker starting...")
    print("Task Queue: batcher-queue")
    print("Workflows: BatcherWorkflow")
    print("Activities: batch_write_to_database")
    print("-" * 50)
    
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_batcher_worker())
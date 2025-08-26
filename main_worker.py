import asyncio
from temporalio.client import Client
from temporalio.worker import Worker

from main_workflow import MainWorkflow
from main_activities import simulate_work


async def run_main_worker():
    """Start the worker for Main workflows"""
    client = await Client.connect("localhost:7233")
    
    worker = Worker(
        client,
        task_queue="main-workflow-queue",
        workflows=[MainWorkflow],
        activities=[simulate_work],
    )
    
    print("🚀 Main Workflow Worker starting...")
    print("Task Queue: main-workflow-queue")
    print("Workflows: MainWorkflow")
    print("Activities: simulate_work")
    print("-" * 50)
    
    await worker.run()


if __name__ == "__main__":
    asyncio.run(run_main_worker())
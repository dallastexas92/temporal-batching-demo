import asyncio
from temporalio import activity


@activity.defn
async def simulate_work(workflow_id: str, step: str) -> str:
    """Simulate some business logic work"""
    # Simulate variable work time (1-3 seconds)
    await asyncio.sleep(1 + hash(workflow_id) % 3)
    
    result = f"Completed {step} for workflow {workflow_id}"
    activity.logger.info(result)
    return result
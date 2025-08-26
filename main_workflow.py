from datetime import timedelta
from typing import Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy

from main_activities import simulate_work


@workflow.defn
class MainWorkflow:
    """
    Main business workflow that needs to write to the database.
    Signals the batcher and waits for confirmation.
    """
    
    def __init__(self):
        self.write_completed = False
        self.write_result = None
        
    @workflow.run
    async def run(self, work_data: str) -> str:
        workflow_id = workflow.info().workflow_id
        
        # Step 1: Do some business work
        step1_result = await workflow.execute_activity(
            simulate_work,
            args=[workflow_id, "initial_processing"],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3)
        )
        
        # Step 2: Need to write to database - signal the batcher
        batcher_handle = workflow.get_external_workflow_handle("batcher-main")
        
        write_request = {
            "workflow_id": workflow_id,
            "data": work_data,
            "requesting_workflow": workflow_id  # For callback
        }
        
        await batcher_handle.signal("add_write_request", write_request)
        workflow.logger.info(f"Signaled batcher with write request: {work_data}")
        
        # Step 3: Wait for write confirmation
        await workflow.wait_condition(lambda: self.write_completed)
        workflow.logger.info(f"Write confirmed! Result: {self.write_result}")
        
        # Step 4: Continue with remaining work
        final_result = await workflow.execute_activity(
            simulate_work,
            args=[workflow_id, "final_processing"],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3)
        )
        
        return f"Workflow {workflow_id} completed: {step1_result} -> DB Write -> {final_result}"
    
    @workflow.signal
    def write_confirmation(self, result: Dict[str, Any]):
        """Receive confirmation that the database write completed"""
        self.write_completed = True
        self.write_result = result
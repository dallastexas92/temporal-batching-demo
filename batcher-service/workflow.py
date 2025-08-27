import asyncio
from datetime import timedelta
from typing import List, Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy

from activities import batch_write_to_database


@workflow.defn  
class BatcherWorkflow:
    """
    Batcher workflow that collects write requests and performs batch writes.
    """
    
    def __init__(self):
        self.pending_writes: List[Dict[str, Any]] = []
        self.batch_size_limit = 100  # Max batch size
        
    @workflow.run
    async def run(self) -> None:
        workflow.logger.info("Batcher workflow started")
        
        while True:
            # Wait for either:
            # 1. 20 second timeout, OR 
            # 2. Batch size limit reached
            try:
                await workflow.wait_condition(
                    lambda: len(self.pending_writes) >= self.batch_size_limit,
                    timeout=timedelta(seconds=20)
                )
            except asyncio.TimeoutError:
                # Timeout reached - process whatever we have
                pass
            
            # Process the batch if we have any pending writes
            if self.pending_writes:
                await self._process_batch()
    
    async def _process_batch(self):
        """Process the current batch of writes"""
        batch_to_process = self.pending_writes.copy()
        self.pending_writes.clear()
        
        workflow.logger.info(f"Processing batch of {len(batch_to_process)} writes")
        
        # Execute the batch write
        try:
            write_result = await workflow.execute_activity(
                batch_write_to_database,
                args=[batch_to_process],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(maximum_attempts=3)
            )
            
            # Signal completion back to each requesting workflow
            for write_request in batch_to_process:
                requesting_workflow_id = write_request["requesting_workflow"]
                try:
                    requesting_handle = workflow.get_external_workflow_handle(requesting_workflow_id)
                    await requesting_handle.signal("write_confirmation", write_result)
                    workflow.logger.info(f"Confirmed write completion to {requesting_workflow_id}")
                except Exception as e:
                    workflow.logger.error(f"Failed to signal {requesting_workflow_id}: {e}")
                    
        except Exception as e:
            workflow.logger.error(f"Batch write failed: {e}")
            # In a real system, you'd want error handling/retry logic here
            
    @workflow.signal
    def add_write_request(self, request: Dict[str, Any]):
        """Receive a write request from a main workflow"""
        self.pending_writes.append(request)
        workflow.logger.info(f"Added write request from {request['workflow_id']} (total pending: {len(self.pending_writes)})")
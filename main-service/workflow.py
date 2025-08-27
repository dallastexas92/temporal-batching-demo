import asyncio
from datetime import timedelta
from typing import Dict, Any, Optional
from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError

from activities import simulate_work


@workflow.defn
class MainWorkflow:
    """
    Transactional main workflow with exactly-once request processing
    """
    
    def __init__(self):
        self.write_completed = False
        self.write_result: Optional[Dict[str, Any]] = None
        self.write_timeout = timedelta(minutes=2)
        self.request_id: Optional[str] = None  # Track our request ID
        
    @workflow.run
    async def run(self, work_data: str) -> str:
        workflow_id = workflow.info().workflow_id
        workflow.logger.info(f"Starting transaction workflow {workflow_id} with data: {work_data}")
        
        # Step 1: Initial business processing
        step1_result = await workflow.execute_activity(
            simulate_work,
            args=[workflow_id, "initial_processing"],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3)
        )
        
        # Step 2: Submit write request to batcher with error handling
        write_submitted = await self._submit_write_request(work_data)
        if not write_submitted:
            raise ApplicationError(
                "Could not submit write request to batcher after retries",
                non_retryable=True,
                type="SIGNAL_DELIVERY_FAILED"
            )
        
        # Step 3: Wait for write confirmation with timeout
        write_confirmed = await self._wait_for_write_confirmation()
        if not write_confirmed:
            raise ApplicationError(
                "Database write was not confirmed within timeout",
                non_retryable=True,
                type="WRITE_CONFIRMATION_TIMEOUT"
            )
        
        # Step 4: Check write result
        if self.write_result.get("status") != "success":
            raise ApplicationError(
                f"Database write failed: {self.write_result}",
                non_retryable=True,
                type="DATABASE_WRITE_FAILED"
            )
        
        # Step 5: Final business processing (only if write was successful)
        final_result = await workflow.execute_activity(
            simulate_work,
            args=[workflow_id, "final_processing"],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3)
        )
        
        success_msg = (f"TRANSACTION_SUCCESS: Workflow {workflow_id} completed: "
                        f"{step1_result} -> DB Write (batch: {self.write_result.get('batch_id', 'unknown')}, "
                        f"request: {self.request_id}) -> {final_result}")
        
        workflow.logger.info(success_msg)
        return success_msg
    
    async def _submit_write_request(self, work_data: str) -> bool:
        """Submit write request to batcher with retry logic and exactly-once guarantees"""
        
        workflow_id = workflow.info().workflow_id
        max_attempts = 3
        
        # Generate a unique request ID for exactly-once processing
        # Use deterministic generation based on workflow ID and run ID to handle retries
        run_id = workflow.info().run_id
        self.request_id = f"{workflow_id}-{run_id}-write-request"
        
        for attempt in range(max_attempts):
            try:
                batcher_handle = workflow.get_external_workflow_handle("batcher-main")
                
                write_request = {
                    "workflow_id": workflow_id,
                    "data": work_data,
                    "requesting_workflow": workflow_id,
                    "request_id": self.request_id,  # CRITICAL: Include request_id for deduplication
                    "submitted_at": workflow.now().isoformat(),
                    "attempt": attempt + 1
                }
                
                await batcher_handle.signal("add_write_request", write_request)
                workflow.logger.info(f"Successfully submitted write request {self.request_id} (attempt {attempt + 1})")
                return True
                
            except ApplicationError as e:
                if "not found" in str(e).lower():
                    workflow.logger.error(f"Batcher workflow not found (attempt {attempt + 1})")
                    if attempt < max_attempts - 1:
                        await workflow.sleep(timedelta(seconds=2 ** attempt))
                        continue
                else:
                    workflow.logger.error(f"Failed to signal batcher (attempt {attempt + 1}): {e}")
                    if attempt < max_attempts - 1:
                        await workflow.sleep(timedelta(seconds=1))
                        continue
                        
            except Exception as e:
                workflow.logger.error(f"Unexpected error signaling batcher (attempt {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    await workflow.sleep(timedelta(seconds=1))
                    continue
        
        workflow.logger.error(f"Failed to submit write request {self.request_id} after {max_attempts} attempts")
        return False
    
    async def _wait_for_write_confirmation(self) -> bool:
        """Wait for write confirmation with proper timeout handling"""
        
        try:
            await workflow.wait_condition(
                lambda: self.write_completed,
                timeout=self.write_timeout
            )
            workflow.logger.info(f"Write confirmed for request {self.request_id}! Result: {self.write_result}")
            return True
            
        except asyncio.TimeoutError:
            # This is the specific timeout exception from wait_condition
            workflow.logger.error(f"TIMEOUT: Write confirmation not received for request {self.request_id} after {self.write_timeout}")
            return False
            
        except Exception as e:
            workflow.logger.error(f"ERROR: Failed waiting for write confirmation for request {self.request_id}: {e}")
            return False
    
    @workflow.signal
    def write_confirmation(self, result: Dict[str, Any]):
        """Enhanced signal handler for write confirmations with validation"""
        
        if not isinstance(result, dict):
            workflow.logger.error("Invalid write confirmation: not a dictionary")
            return
        
        # Log the confirmation with details
        batch_id = result.get("batch_id", "unknown")
        status = result.get("status", "unknown")
        count = result.get("count", 0)
        
        workflow.logger.info(f"Received write confirmation for request {self.request_id} - "
                           f"Batch: {batch_id}, Status: {status}, Count: {count}")
        
        self.write_completed = True
        self.write_result = result
    
    @workflow.query
    def get_status(self) -> Dict[str, Any]:
        """Query method to check workflow status"""
        return {
            "workflow_id": workflow.info().workflow_id,
            "request_id": self.request_id,
            "write_completed": self.write_completed,
            "write_result": self.write_result,
            "current_time": workflow.now().isoformat()
        }
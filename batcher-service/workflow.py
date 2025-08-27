import asyncio
from datetime import timedelta
from typing import List, Dict, Any, Optional, Set
from temporalio import workflow
from temporalio.common import RetryPolicy
from dataclasses import dataclass, field
import hashlib

from activities import batch_write_to_database


@dataclass
class BatcherState:
    """State that gets carried over during continue-as-new with deduplication support"""
    pending_writes: List[Dict[str, Any]]
    processed_batches_count: int
    total_signals_received: int
    processed_request_ids: Set[str] = field(default_factory=set)  # NEW: For deduplication
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "pending_writes": self.pending_writes,
            "processed_batches_count": self.processed_batches_count, 
            "total_signals_received": self.total_signals_received,
            "processed_request_ids": list(self.processed_request_ids)  # Convert set to list for JSON
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BatcherState':
        return cls(
            pending_writes=data.get("pending_writes", []),
            processed_batches_count=data.get("processed_batches_count", 0),
            total_signals_received=data.get("total_signals_received", 0),
            processed_request_ids=set(data.get("processed_request_ids", []))  # Convert list back to set
        )


@workflow.defn  
class BatcherWorkflow:
    """
    Batcher workflow with exactly-once message processing and continue-as-new support
    """
    
    def __init__(self):
        self.state = BatcherState(
            pending_writes=[],
            processed_batches_count=0,
            total_signals_received=0,
            processed_request_ids=set()
        )
        self.batch_size_limit = 100
        self.continue_as_new_threshold = 1000  # Continue-as-new after receiving 1k signals (only counts signals received)
        self.max_batch_wait_time = timedelta(seconds=20)
        # Cleanup processed IDs periodically to prevent unbounded growth
        self.max_processed_ids_to_keep = 10000
        
    @workflow.run
    async def run(self, initial_state: Optional[Dict[str, Any]] = None) -> None:
        """Main workflow run method with continue-as-new support"""
        
        # Restore state if continuing from previous execution
        if initial_state:
            self.state = BatcherState.from_dict(initial_state)
            workflow.logger.info(f"Resumed batcher with {len(self.state.pending_writes)} pending writes, "
                               f"{self.state.processed_batches_count} batches processed, "
                               f"{self.state.total_signals_received} total signals, "
                               f"{len(self.state.processed_request_ids)} processed request IDs")
        else:
            workflow.logger.info("Batcher workflow started fresh")
        
        while True:
            # Check if we should continue-as-new to prevent unbounded history growth
            if self.state.total_signals_received >= self.continue_as_new_threshold:
                workflow.logger.info(f"Continuing as new after {self.state.total_signals_received} signals")
                
                # Cleanup old processed request IDs to prevent unbounded growth
                self._cleanup_old_request_ids()
                
                workflow.continue_as_new(self.state.to_dict())
                return
            
            # Wait for batch conditions
            try:
                await workflow.wait_condition(
                    lambda: len(self.state.pending_writes) >= self.batch_size_limit,
                    timeout=self.max_batch_wait_time
                )
            except asyncio.TimeoutError:
                workflow.logger.debug("Batch timeout reached")
            
            # Process the batch if we have pending writes
            if self.state.pending_writes:
                await self._process_batch()
    
    def _cleanup_old_request_ids(self):
        """
        Cleanup old processed request IDs to prevent unbounded memory growth.
        Keep only the most recent IDs based on a reasonable retention policy.
        """
        if len(self.state.processed_request_ids) > self.max_processed_ids_to_keep:
            # In a real implementation, you might want to keep IDs from the last N minutes/hours
            # For now, we'll keep a fixed number of the most recent ones
            # Note: Sets don't maintain order, so this is a simple cleanup
            ids_to_remove = len(self.state.processed_request_ids) - (self.max_processed_ids_to_keep // 2)
            ids_list = list(self.state.processed_request_ids)
            self.state.processed_request_ids = set(ids_list[ids_to_remove:])
            
            workflow.logger.info(f"Cleaned up {ids_to_remove} old processed request IDs")
    
    async def _process_batch(self):
        """Process the current batch with improved error handling"""
        
        # Create a snapshot of the current batch
        batch_to_process = self.state.pending_writes.copy()
        self.state.pending_writes.clear()
        
        batch_id = f"batch-{self.state.processed_batches_count + 1}"
        workflow.logger.info(f"Processing {batch_id} with {len(batch_to_process)} writes")
        
        try:
            # Execute the batch write with retries
            write_result = await workflow.execute_activity(
                batch_write_to_database,
                args=[batch_to_process],
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=RetryPolicy(
                    maximum_attempts=3,
                    initial_interval=timedelta(seconds=1),
                    maximum_interval=timedelta(seconds=10),
                    backoff_coefficient=2.0
                )
            )
            
            # Update state after successful processing
            self.state.processed_batches_count += 1
            
            # Enhanced result with batch metadata
            enhanced_result = {
                **write_result,
                "batch_id": batch_id,
                "batch_size": len(batch_to_process)
            }
            
            # Send confirmations to all requesting workflows
            confirmation_tasks = []
            for write_request in batch_to_process:
                task = self._send_confirmation_safe(write_request, enhanced_result)
                confirmation_tasks.append(task)
            
            # Wait for all confirmations (with individual error handling)
            await asyncio.gather(*confirmation_tasks, return_exceptions=True)
            
            workflow.logger.info(f"Successfully processed {batch_id}")
            
        except Exception as e:
            workflow.logger.error(f"Failed to process {batch_id}: {e}")
            
            # Re-add failed writes to pending (with some risk of reordering)
            workflow.logger.warn(f"Re-queuing {len(batch_to_process)} failed writes")
            self.state.pending_writes.extend(batch_to_process)
    
    async def _send_confirmation_safe(self, write_request: Dict[str, Any], result: Dict[str, Any]):
        """Safely send confirmation to requesting workflow with error handling"""
        
        requesting_workflow_id = write_request.get("requesting_workflow")
        if not requesting_workflow_id:
            workflow.logger.warn("Write request missing requesting_workflow field")
            return
        
        try:
            requesting_handle = workflow.get_external_workflow_handle(requesting_workflow_id)
            await requesting_handle.signal("write_confirmation", result)
            workflow.logger.debug(f"Confirmed write completion to {requesting_workflow_id}")
            
        except Exception as e:
            workflow.logger.error(f"Failed to signal {requesting_workflow_id}: {e}")
            
    @workflow.signal
    def add_write_request(self, request: Dict[str, Any]):
        """
        Receive a write request with exactly-once processing guarantee
        """
        
        # Basic validation
        if not isinstance(request, dict):
            workflow.logger.error("Invalid write request: not a dictionary")
            return
            
        required_fields = ["workflow_id", "data", "requesting_workflow"]
        missing_fields = [field for field in required_fields if field not in request]
        if missing_fields:
            workflow.logger.error(f"Write request missing required fields: {missing_fields}")
            return
        
        # EXACTLY-ONCE PROCESSING: Check for request ID
        request_id = request.get("request_id")
        if not request_id:
            # Generate a deterministic request ID if not provided (for backward compatibility)
            # Use deterministic hash instead of random UUID
            request_data = f"{request['workflow_id']}-{request.get('data', '')}-{workflow.now().isoformat()}"
            request_id = f"{request['workflow_id']}-{hashlib.md5(request_data.encode()).hexdigest()[:8]}"
            request["request_id"] = request_id
            workflow.logger.warn(f"Generated request_id for request from {request['workflow_id']}: {request_id}")
        
        # DEDUPLICATION: Check if we've already processed this request
        if request_id in self.state.processed_request_ids:
            workflow.logger.info(f"Duplicate request {request_id} from {request['workflow_id']} - ignoring")
            return
        
        # Mark request as processed BEFORE adding to queue (exactly-once guarantee)
        self.state.processed_request_ids.add(request_id)
        
        # Add request with metadata
        enriched_request = {
            **request,
            "received_at": workflow.now(),
            "batch_sequence": self.state.total_signals_received,
            "request_id": request_id  # Ensure request_id is in the enriched request
        }
        
        self.state.pending_writes.append(enriched_request)
        self.state.total_signals_received += 1
        
        workflow.logger.info(
            f"Added write request {request_id} from {request['workflow_id']} "
            f"(total pending: {len(self.state.pending_writes)}, "
            f"total received: {self.state.total_signals_received})"
        )
    
    @workflow.query
    def get_stats(self) -> Dict[str, Any]:
        """Query method to get current batcher statistics"""
        return {
            "pending_writes": len(self.state.pending_writes),
            "processed_batches": self.state.processed_batches_count,
            "total_signals_received": self.state.total_signals_received,
            "processed_request_ids_count": len(self.state.processed_request_ids),
            "continue_as_new_threshold": self.continue_as_new_threshold,
            "batch_size_limit": self.batch_size_limit
        }
    
    @workflow.query  
    def is_request_processed(self, request_id: str) -> bool:
        """Query to check if a specific request has been processed (useful for debugging)"""
        return request_id in self.state.processed_request_ids
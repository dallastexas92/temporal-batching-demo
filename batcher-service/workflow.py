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
    # Note: total_signals_received removed - we'll use a session counter instead
    processed_request_ids: Set[str] = field(default_factory=set)  # Only for pending requests
    continue_as_new_count: int = 0  # Safety counter to prevent infinite loops
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "pending_writes": self.pending_writes,
            "processed_batches_count": self.processed_batches_count,
            "processed_request_ids": list(self.processed_request_ids),  # Convert set to list for JSON
            "continue_as_new_count": self.continue_as_new_count
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BatcherState':
        return cls(
            pending_writes=data.get("pending_writes", []),
            processed_batches_count=data.get("processed_batches_count", 0),
            processed_request_ids=set(data.get("processed_request_ids", [])),  # Convert list back to set
            continue_as_new_count=data.get("continue_as_new_count", 0)
        )


@workflow.defn  
class BatcherWorkflow:
    """
    Batcher workflow with exactly-once message processing and improved continue-as-new support
    """
    
    def __init__(self):
        self.state = BatcherState(
            pending_writes=[],
            processed_batches_count=0,
            processed_request_ids=set(),
            continue_as_new_count=0
        )
        self.batch_size_limit = 100
        # Session-level counter (resets on each continue-as-new)
        self.session_signals_received = 0
        self.max_batch_wait_time = timedelta(seconds=20)
        # Safety limits
        self.max_continue_as_new_cycles = 10  # Prevent runaway continue-as-new
        
    @workflow.run
    async def run(self, initial_state: Optional[Dict[str, Any]] = None) -> None:
        """Main workflow run method with improved continue-as-new support"""
        
        # Restore state if continuing from previous execution
        if initial_state:
            self.state = BatcherState.from_dict(initial_state)
            workflow.logger.info(f"Resumed batcher with {len(self.state.pending_writes)} pending writes, "
                               f"{self.state.processed_batches_count} batches processed, "
                               f"{len(self.state.processed_request_ids)} tracked request IDs, "
                               f"continue-as-new cycle {self.state.continue_as_new_count}")
            
            # Safety check: Prevent infinite continue-as-new loops
            if self.state.continue_as_new_count >= self.max_continue_as_new_cycles:
                workflow.logger.error(f"Maximum continue-as-new cycles ({self.max_continue_as_new_cycles}) reached. "
                                    "Forcing batch processing and resetting counter.")
                # Force process all pending writes to break the cycle
                if self.state.pending_writes:
                    await self._process_batch()
                # Reset counter to allow normal operation
                self.state.continue_as_new_count = 0
        else:
            workflow.logger.info("Batcher workflow started fresh")
        
        while True:
            # Check if we should continue-as-new
            if self._should_continue_as_new():
                await self._safe_continue_as_new()
                return  # This execution ends here
            
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
    
    def _should_continue_as_new(self) -> bool:
        """Determine if workflow should continue-as-new using recommended patterns"""
        
        # Primary: Use Temporal's built-in suggestion (recommended approach)
        if workflow.info().is_continue_as_new_suggested():
            workflow.logger.info("Continue-as-new suggested by Temporal")
            return True
        
        # Safety: Large pending queue (prevent unbounded state growth)
        if len(self.state.pending_writes) > self.batch_size_limit * 10:  # 1000 pending writes
            workflow.logger.warn(f"Continue-as-new triggered by large pending queue: {len(self.state.pending_writes)}")
            return True
            
        return False
    
    async def _safe_continue_as_new(self):
        """Safely continue-as-new with proper cleanup and state management"""
        
        workflow.logger.info("Preparing for continue-as-new...")
        
        # Step 1: Wait for any running signal handlers to complete
        try:
            await workflow.wait_condition(
                lambda: workflow.all_handlers_finished(),
                timeout=timedelta(seconds=10)
            )
            workflow.logger.debug("All signal handlers finished")
        except asyncio.TimeoutError:
            workflow.logger.warn("Timeout waiting for signal handlers - continuing anyway")
        
        # Step 2: Process any pending batch to reduce state size
        if self.state.pending_writes:
            workflow.logger.info(f"Processing {len(self.state.pending_writes)} pending writes before continue-as-new")
            await self._process_batch()
        
        # Step 3: Clean up deduplication state
        # Only keep request IDs for truly pending requests (should be empty after processing)
        pending_request_ids = {req['request_id'] for req in self.state.pending_writes if 'request_id' in req}
        old_count = len(self.state.processed_request_ids)
        self.state.processed_request_ids = pending_request_ids
        workflow.logger.info(f"Cleaned up deduplication state: {old_count} -> {len(self.state.processed_request_ids)} request IDs")
        
        # Step 4: Prepare state for next execution
        self.state.continue_as_new_count += 1
        continue_state = self.state.to_dict()
        
        workflow.logger.info(f"Continuing as new (cycle {self.state.continue_as_new_count})")
        workflow.continue_as_new(continue_state)
    
    async def _process_batch(self):
        """Process the current batch with improved deduplication cleanup"""
        
        # Create a snapshot of the current batch
        batch_to_process = self.state.pending_writes.copy()
        batch_request_ids = {req['request_id'] for req in batch_to_process if 'request_id' in req}
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
            
            # SUCCESS: Update state and clean up deduplication
            self.state.processed_batches_count += 1
            
            # CRITICAL: Remove successfully processed request IDs from deduplication set
            self.state.processed_request_ids -= batch_request_ids
            workflow.logger.debug(f"Removed {len(batch_request_ids)} request IDs from deduplication set")
            
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
            
            # FAILURE: Re-add failed writes to pending (keep request IDs in deduplication set)
            workflow.logger.warn(f"Re-queuing {len(batch_to_process)} failed writes")
            self.state.pending_writes.extend(batch_to_process)
            # Note: We don't remove request_ids from processed_request_ids on failure
            # This ensures we won't accept duplicates during retry
    
    async def _send_confirmation_safe(self, write_request: Dict[str, Any], result: Dict[str, Any]):
        """Safely send confirmation to requesting workflow with error handling"""
        
        requesting_workflow_id = write_request.get("requesting_workflow")
        if not requesting_workflow_id:
            workflow.logger.warn("Write request missing requesting_workflow field")
            return
        
        # TODO: Implement retry logic for confirmation delivery to prevent workflow timeouts
        # when batch write to DB succeeds but confirmation signal fails
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
            request_data = f"{request['workflow_id']}-{request.get('data', '')}-{workflow.now().isoformat()}"
            request_id = f"{request['workflow_id']}-{hashlib.md5(request_data.encode()).hexdigest()[:8]}"
            request["request_id"] = request_id
            workflow.logger.warn(f"Generated request_id for request from {request['workflow_id']}: {request_id}")
        
        # DEDUPLICATION: Check if we've already processed this request
        if request_id in self.state.processed_request_ids:
            workflow.logger.info(f"Duplicate request {request_id} from {request['workflow_id']} - ignoring")
            return
        
        # Mark request as being processed (add to deduplication set)
        self.state.processed_request_ids.add(request_id)
        
        # Add request with metadata
        enriched_request = {
            **request,
            "received_at": workflow.now(),
            "batch_sequence": self.session_signals_received,  # Session-level sequence
            "request_id": request_id
        }
        
        self.state.pending_writes.append(enriched_request)
        self.session_signals_received += 1  # Increment session counter (resets on continue-as-new)
        
        workflow.logger.info(
            f"Added write request {request_id} from {request['workflow_id']} "
            f"(pending: {len(self.state.pending_writes)}, "
            f"session signals: {self.session_signals_received})"
        )
    
    @workflow.query
    def get_stats(self) -> Dict[str, Any]:
        """Query method to get current batcher statistics"""
        return {
            "pending_writes": len(self.state.pending_writes),
            "processed_batches": self.state.processed_batches_count,
            "session_signals_received": self.session_signals_received,  # Session counter
            "processed_request_ids_count": len(self.state.processed_request_ids),
            "continue_as_new_cycle": self.state.continue_as_new_count,
            "batch_size_limit": self.batch_size_limit,
            "is_continue_suggested": workflow.info().is_continue_as_new_suggested()
        }
    
    @workflow.query  
    def is_request_processed(self, request_id: str) -> bool:
        """Query to check if a specific request has been processed (useful for debugging)"""
        return request_id in self.state.processed_request_ids
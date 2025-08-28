import asyncio
from datetime import datetime
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.exceptions import ApplicationError
from workflow import BatcherWorkflow


class BatcherManager:
    """Manager for the batcher workflow with health monitoring"""
    
    def __init__(self, client: Client):
        self.client = client
        self.batcher_id = "batcher-main"
        self.task_queue = "batcher-queue"
        self.health_check_interval = 30  # seconds
        
    async def start_or_resume_batcher(self):
        """Start batcher or resume if already running"""
        
        try:
            # Try to start a fresh batcher
            batcher_handle = await self.client.start_workflow(
                BatcherWorkflow.run,
                id=self.batcher_id,
                task_queue=self.task_queue
            )
            
            print(f"✅ New batcher started: {batcher_handle.id}")
            return batcher_handle
            
        except Exception as e:
            if "already started" in str(e).lower() or "already exists" in str(e).lower():
                print("ℹ️  Batcher workflow already running, getting handle...")
                batcher_handle = self.client.get_workflow_handle(self.batcher_id)
                
                # Check if it's actually running
                description = await batcher_handle.describe()
                if description.status == WorkflowExecutionStatus.RUNNING:
                    print("✅ Connected to existing running batcher")
                    return batcher_handle
                else:
                    print(f"⚠️  Existing batcher is in status: {description.status}")
                    return None
            else:
                print(f"❌ Error starting batcher: {e}")
                return None
    
    async def monitor_batcher_health(self, batcher_handle):
        """Monitor batcher health and display statistics"""
        
        print("\n🔄 Starting batcher health monitoring...")
        print("=" * 70)
        
        while True:
            try:
                # Query batcher statistics
                stats = await batcher_handle.query("get_stats")
                current_time = datetime.now().strftime("%H:%M:%S")
                
                print(f"[{current_time}] Batcher Stats:")
                print(f"  📝 Pending writes: {stats['pending_writes']}")
                print(f"  ✅ Processed batches: {stats['processed_batches']}")
                print(f"  📨 Session signals: {stats['session_signals_received']}")
                print(f"  🆔 Tracked request IDs: {stats['processed_request_ids_count']}")
                print(f"  🔄 Continue-as-new cycle: {stats['continue_as_new_cycle']}")
                print(f"  💡 Temporal suggests continue: {stats['is_continue_suggested']}")
                print("-" * 50)
                
                # Check if batcher is still running
                description = await batcher_handle.describe()
                if description.status != WorkflowExecutionStatus.RUNNING:
                    print(f"⚠️  Batcher status changed to: {description.status}")
                    
                    if description.status == WorkflowExecutionStatus.CONTINUED_AS_NEW:
                        print("🔄 Batcher continued-as-new, updating handle...")
                        # Get the new execution handle
                        batcher_handle = self.client.get_workflow_handle(
                            self.batcher_id,
                            run_id=description.latest_execution_run_id
                        )
                        print("✅ Updated to new execution handle")
                    else:
                        print("❌ Batcher is no longer running")
                        break
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                print(f"❌ Error monitoring batcher health: {e}")
                await asyncio.sleep(self.health_check_interval)
    
    async def run(self):
        """Main run method"""
        
        print("🎯 Starting Batcher Management System (v2.0)")
        print("=" * 60)
        print("🔧 Improvements:")
        print("  • Fixed infinite continue-as-new loops")
        print("  • Session-based signal counting")
        print("  • Proper deduplication cleanup")
        print("  • Uses Temporal's continue-as-new suggestions")
        print("  • Safety mechanisms for runaway cycles")
        print("=" * 60)
        
        batcher_handle = await self.start_or_resume_batcher()
        if not batcher_handle:
            print("❌ Failed to start or connect to batcher")
            return
        
        print("=" * 60)
        print("🌐 View in Temporal Web UI: http://localhost:8233")
        print("💡 Press Ctrl+C to stop monitoring")
        print("=" * 60)
        
        try:
            await self.monitor_batcher_health(batcher_handle)
        except KeyboardInterrupt:
            print("\n👋 Stopping batcher monitoring...")
        except Exception as e:
            print(f"\n❌ Error in batcher management: {e}")


async def main():
    """Main entry point"""
    try:
        client = await Client.connect("localhost:7233")
        manager = BatcherManager(client)
        await manager.run()
        
    except KeyboardInterrupt:
        print("\n👋 Batcher management interrupted")
    except Exception as e:
        print(f"❌ Fatal error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
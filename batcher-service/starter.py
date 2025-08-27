import asyncio
from temporalio.client import Client
from workflow import BatcherWorkflow


async def start_batcher():
    """Start the batcher workflow"""
    client = await Client.connect("localhost:7233")
    
    print("🎯 Starting batcher workflow")
    print("=" * 40)
    
    try:
        batcher_handle = await client.start_workflow(
            BatcherWorkflow.run,
            id="batcher-main",
            task_queue="batcher-queue"
        )
        
        print(f"   ✅ Batcher started: {batcher_handle.id}")
        print("=" * 40)
        print("🔄 Batcher is running and waiting for requests...")
        print(f"🌐 View in Temporal Web UI: http://localhost:8233")
        print("💡 Press Ctrl+C to stop")
        print("=" * 40)
        
        # Keep the script running (batcher runs indefinitely)
        try:
            while True:
                await asyncio.sleep(10)
                # Could add health checks or metrics here
        except KeyboardInterrupt:
            print("\n👋 Stopping batcher...")
            
    except Exception as e:
        if "already started" in str(e).lower():
            print("ℹ️  Batcher workflow is already running")
        else:
            print(f"❌ Error starting batcher: {e}")


def main():
    try:
        asyncio.run(start_batcher())
    except KeyboardInterrupt:
        print("\n👋 Batcher startup interrupted")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
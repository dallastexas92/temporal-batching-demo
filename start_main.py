import asyncio
import argparse
from temporalio.client import Client
from main_workflow import MainWorkflow


async def start_main_workflows(num_workflows: int):
    """Start N main workflows"""
    client = await Client.connect("localhost:7233")
    
    print(f"🎯 Starting {num_workflows} main workflows")
    print("=" * 50)
    
    main_workflow_handles = []
    
    for i in range(num_workflows):
        workflow_id = f"main-workflow-{i:03d}"
        work_data = f"business-data-{i}"
        
        handle = await client.start_workflow(
            MainWorkflow.run,
            work_data,
            id=workflow_id,
            task_queue="main-workflow-queue"
        )
        
        main_workflow_handles.append(handle)
        print(f"   ✅ Started: {workflow_id}")
    
    print("=" * 50)
    print("🔄 Main workflows started!")
    print(f"🌐 View in Temporal Web UI: http://localhost:8233")
    print("=" * 50)
    
    # Wait for all workflows to complete
    print("⏳ Waiting for all workflows to complete...")
    results = await asyncio.gather(
        *[handle.result() for handle in main_workflow_handles],
        return_exceptions=True
    )
    
    print("\n" + "=" * 50)
    print("🎉 WORKFLOW RESULTS")
    print("=" * 50)
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"❌ Workflow {i:03d}: ERROR - {result}")
        else:
            print(f"✅ Workflow {i:03d}: {result}")
    
    print("=" * 50)
    print("🏁 Main workflows completed!")


def main():
    parser = argparse.ArgumentParser(description="Start Main Workflows")
    parser.add_argument(
        "--workflows", 
        type=int, 
        default=5,
        help="Number of main workflows to start (default: 5)"
    )
    
    args = parser.parse_args()
    
    if args.workflows < 1:
        print("❌ Number of workflows must be at least 1")
        return
    
    try:
        asyncio.run(start_main_workflows(args.workflows))
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
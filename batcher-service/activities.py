import asyncio
from typing import List, Dict, Any
from temporalio import activity


@activity.defn
async def batch_write_to_database(write_requests: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Simulate batch writing to database by printing to console.
    Returns metadata about the write operation.
    """
    if not write_requests:
        return {"status": "no_data", "count": 0}
    
    print("\n" + "="*60)
    print(f"📊 BATCH WRITE TO DATABASE - {len(write_requests)} records")
    print("="*60)
    
    for i, request in enumerate(write_requests, 1):
        print(f"{i:2d}. Workflow: {request['workflow_id']} | Data: {request['data']}")
    
    print("="*60)
    print(f"✅ Successfully wrote {len(write_requests)} records to ledger")
    print("="*60 + "\n")
    
    # Simulate database write time
    await asyncio.sleep(0.5)
    
    return {
        "status": "success",
        "count": len(write_requests),
        "write_time": "simulated_timestamp"
    }
import asyncio
import time
from typing import AsyncIterable, Dict, Any
from conveyor import single_task, batch_task, Pipeline

# Decorate our source generator to make it compatible with the pipeline
@single_task
async def generate_batches_with_delay() -> AsyncIterable[Dict[str, Any]]:
    """
    Source generator that simulates fetching data in batches with delays.
    No input parameters - this is a zero-input source.
    
    Each batch takes 3 seconds to "fetch" and contains 5 items.
    """
    print("🔄 Starting batch generation (zero-input source)...")
    
    for batch_num in range(3):  # Generate 3 batches
        print(f"🔍 Fetching batch {batch_num+1}...")
        start_time = time.time()
        
        # Simulate a long fetch operation (e.g., API call, database query)
        await asyncio.sleep(3)
        
        fetch_time = time.time() - start_time
        print(f"✅ Batch {batch_num+1} fetched after {fetch_time:.2f}s")
        
        # Generate 5 items per batch
        for item_idx in range(5):
            item_value = batch_num * 10 + item_idx
            result = {
                "id": item_value,
                "batch": batch_num,
                "data": f"Payload for item {item_value}",
                "fetched_at": time.time()
            }
            print(f"  → Yielding item {item_value} from batch {batch_num+1}")
            yield result

@batch_task(max_size=3, min_size=1)
async def process_batch(items: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    """
    Process items in batches of maximum 3 items.
    Each batch processing takes 1 second.
    """
    batch_ids = [item["id"] for item in items]
    start_time = time.time()
    
    print(f"🔄 Processing batch with items: {batch_ids}")
    
    # Simulate processing time
    await asyncio.sleep(1)
    
    process_time = time.time() - start_time
    
    # Add processing metadata to each item
    for item in items:
        item["processed"] = True
        item["process_time"] = process_time
        item["processed_at"] = time.time()
        item["processed_with"] = batch_ids
    
    print(f"✅ Processed batch {batch_ids} in {process_time:.2f}s")
    
    return items

@single_task
async def add_timestamp(item: Dict[str, Any]) -> Dict[str, Any]:
    """Add final timestamp to track when items emerge from the pipeline."""
    item["emerged_at"] = time.time()
    item["total_time"] = item["emerged_at"] - item["fetched_at"]
    return item

async def main():
    """
    Demonstrate a pipeline that starts with a zero-input source generator
    and processes items in batches.
    """
    print("=" * 70)
    print("ZERO-INPUT PIPELINE WITH BATCH PROCESSING DEMONSTRATION")
    print("=" * 70)
    print("\nThis example shows a pipeline that:")
    print("1. Starts with a source that generates data on its own (no input)")
    print("2. The source generates 3 batches of 5 items each, with 3s delay between batches")
    print("3. Items are then processed in batches of max 3 items, with 1s processing time")
    print("4. We track timing to see the streaming behavior")
    print("\n" + "=" * 70 + "\n")
    
    # Record the start time
    start_time = time.time()
    
    # Create a pipeline starting with our source generator
    pipeline = generate_batches_with_delay | process_batch | add_timestamp
    
    # Collect results and analyze timing
    all_results = []
    batch_yield_times = []
    last_yield_time = None
    
    print("Starting pipeline execution...\n")
    
    # Execute the pipeline - note we don't provide any input data
    # since our source generator doesn't require any
    async for result in pipeline():
        current_time = time.time() - start_time
        
        # Track when items emerge from the pipeline
        if last_yield_time is None or current_time - last_yield_time > 0.5:
            # This looks like a new batch
            batch_yield_times.append(current_time)
        
        last_yield_time = current_time
        
        print(f"📤 Result: Item {result['id']} emerged at {current_time:.2f}s " +
              f"(total time: {result['total_time']:.2f}s)")
        
        all_results.append(result)
    
    # Final analysis
    total_time = time.time() - start_time
    
    print("\n" + "=" * 70)
    print("RESULTS ANALYSIS")
    print("=" * 70)
    
    # Group results by batches they were processed in
    processing_batches = {}
    for result in all_results:
        batch_key = str(result["processed_with"])
        if batch_key not in processing_batches:
            processing_batches[batch_key] = []
        processing_batches[batch_key].append(result["id"])
    
    print("\nProcessing batches:")
    for batch, items in processing_batches.items():
        print(f"  • Batch {batch}: {items}")
    
    print("\nYield timing analysis:")
    for i, yield_time in enumerate(batch_yield_times):
        print(f"  • Batch {i+1} yielded at: {yield_time:.2f}s")
    
    if len(batch_yield_times) >= 2:
        first_batch_time = batch_yield_times[0]
        second_batch_time = batch_yield_times[1]
        print(f"\n  • First batch took: {first_batch_time:.2f}s")
        print(f"  • Time between first and second batch: {second_batch_time - first_batch_time:.2f}s")
    
    print("\nExpected timeline explanation:")
    print("  • 0s: Pipeline starts")
    print("  • ~3s: First batch of 5 items fetched from source")
    print("  • ~4s: First processing batch (3 items) completes and yields")
    print("  • ~5s: Second processing batch (2 items) from first fetch completes and yields")
    print("  • ~6s: Second batch of 5 items fetched from source")
    print("  • ~7s: Third processing batch (3 items) completes and yields")
    print("  • ~8s: Fourth processing batch (2 items) completes and yields")
    print("  • ~9s: Third batch of 5 items fetched from source")
    print("  • ~10s: Fifth processing batch (3 items) completes and yields")
    print("  • ~11s: Sixth processing batch (2 items) completes and yields")
    
    print(f"\nActual total execution time: {total_time:.2f}s")
    
    return {
        "results": all_results,
        "batch_yield_times": batch_yield_times,
        "total_time": total_time
    }

if __name__ == "__main__":
    asyncio.run(main())
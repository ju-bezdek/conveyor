import asyncio
import time
from typing import AsyncIterable
from conveyor import single_task, Pipeline

# Better scenario: processing times from fastest to slowest with a slow outlier
# This demonstrates the streaming advantage better
PROCESSING_TASKS = [
    ("Quick validation", 0.1),           # 1st - Fast (yields immediately)
    ("Basic check", 0.2),                # 2nd - Fast (yields immediately after 1st)  
    ("Simple transformation", 0.3),      # 3rd - Medium (yields after 2nd)
    ("Data enrichment", 0.4),            # 4th - Medium (yields after 3rd)
    ("Complex validation", 0.5),         # 5th - Medium-slow (yields after 4th)
    ("SLOW OUTLIER PROCESS", 2.0),       # 6th - VERY SLOW (blocks remaining items)
    ("Final cleanup", 0.7),              # 7th - Must wait for outlier
    ("Report generation", 0.8),          # 8th - Must wait for outlier
]

@single_task
async def process_task(item: tuple[str, float]) -> dict:
    """Simulate task processing with variable delays."""
    task_name, processing_time = item
    start_time = time.time()
    
    print(f"🔄 Starting: '{task_name}' (expected {processing_time}s)")
    await asyncio.sleep(processing_time)
    
    end_time = time.time()
    actual_time = end_time - start_time
    
    result = {
        'task': task_name,
        'expected_time': processing_time,
        'actual_time': round(actual_time, 3),
        'completed_at': round(end_time, 3)
    }
    
    print(f"✅ Completed: '{task_name}' in {actual_time:.3f}s")
    return result

@single_task
async def add_processing_metadata(result: dict) -> dict:
    """Add metadata about processing order and timing."""
    result['processed_at'] = round(time.time(), 3)
    result['status'] = 'processed'
    return result

async def demonstrate_streaming_benefits():
    """Show the key benefit: early results available while preserving order."""
    
    print("=== 🚀 CONVEYOR STREAMING PERFORMANCE DEMONSTRATION ===\n")
    
    # Create the pipeline
    pipeline = process_task | add_processing_metadata
    
    print("📋 Processing Scenario (designed to show streaming benefits):")
    print("   Tasks arranged from fastest to slowest with a SLOW OUTLIER")
    print()
    for i, (task, time_val) in enumerate(PROCESSING_TASKS, 1):
        marker = " ⚠️  SLOW OUTLIER!" if time_val >= 2.0 else ""
        print(f"   {i}. '{task}' ({time_val}s){marker}")
    print()
    
    print("🎯 EXPECTED BEHAVIOR:")
    print("   • First 5 results should stream out quickly (0.1s → 0.5s)")
    print("   • Then WAIT at position 6 due to slow outlier (2.0s)")
    print("   • Final 2 results yield immediately after outlier completes")
    print("   • Order is PRESERVED throughout")
    print()
    
    # Streaming approach demonstration
    print("🚀 STREAMING PROCESSING (Conveyor's ordered queue approach):")
    print("-" * 70)
    
    streaming_start = time.time()
    streaming_results = []
    yield_times = []
    
    async for result in pipeline(PROCESSING_TASKS):
        current_time = time.time()
        elapsed = current_time - streaming_start
        
        yield_times.append(elapsed)
        streaming_results.append(result)
        
        # Highlight the streaming benefit
        if len(streaming_results) <= 5:
            print(f"📤 Result {len(streaming_results)}: '{result['task']}' at {elapsed:.3f}s ⚡ STREAMING!")
        elif result['task'] == "SLOW OUTLIER PROCESS":
            print(f"📤 Result {len(streaming_results)}: '{result['task']}' at {elapsed:.3f}s 🐌 OUTLIER COMPLETED")
        else:
            print(f"📤 Result {len(streaming_results)}: '{result['task']}' at {elapsed:.3f}s ⚡ IMMEDIATE (buffered)")
    
    streaming_total = time.time() - streaming_start
    
    # Analysis
    print(f"\n📊 STREAMING PERFORMANCE ANALYSIS:")
    print("-" * 70)
    
    # Calculate when we got early results vs when we'd get them with traditional approach
    first_result_time = yield_times[0]
    fifth_result_time = yield_times[4] if len(yield_times) >= 5 else yield_times[-1]
    outlier_completion_time = yield_times[5] if len(yield_times) >= 6 else streaming_total
    
    print(f"⚡ Early Results Performance:")
    print(f"   • 1st result available: {first_result_time:.3f}s (vs traditional: {streaming_total:.3f}s)")
    print(f"   • 5th result available: {fifth_result_time:.3f}s (vs traditional: {streaming_total:.3f}s)")
    print(f"   • Outlier completed: {outlier_completion_time:.3f}s")
    print(f"   • All results completed: {streaming_total:.3f}s")
    print()
    
    improvement_1st = ((streaming_total - first_result_time) / streaming_total) * 100
    improvement_5th = ((streaming_total - fifth_result_time) / streaming_total) * 100
    
    print(f"🎯 STREAMING ADVANTAGES:")
    print(f"   • First result: {improvement_1st:.1f}% faster than waiting for all")
    print(f"   • First 5 results: {improvement_5th:.1f}% faster than waiting for all")
    print(f"   • Subsequent pipeline stages can start processing early results immediately")
    print(f"   • Better user experience with progressive result availability")
    print(f"   • Order preservation maintained throughout")
    print()
    
    # Show the waiting behavior for order preservation
    print(f"🔒 ORDER PRESERVATION BEHAVIOR:")
    print(f"   • Results 1-5: Streamed immediately as they completed")
    print(f"   • Results 6-8: Result 6 (outlier) blocks results 7-8 until completion")
    print(f"   • Once outlier completes, remaining buffered results yield immediately")
    print()
    
    # Timeline analysis
    print(f"⏰ DETAILED TIMELINE:")
    for i, (result_time, result) in enumerate(zip(yield_times, streaming_results)):
        if i < 5:
            timing_note = "immediate"
        elif result['task'] == "SLOW OUTLIER PROCESS":
            timing_note = "slow outlier"
        else:
            timing_note = "immediate (was buffered)"
        print(f"   {i+1}. {result_time:.3f}s: '{result['task']}' ({timing_note})")
    
    return {
        'streaming_time': streaming_total,
        'first_result_time': first_result_time,
        'fifth_result_time': fifth_result_time,
        'improvement_1st': improvement_1st,
        'improvement_5th': improvement_5th,
        'yield_times': yield_times
    }

async def demonstrate_traditional_comparison():
    """Simulate what traditional batch processing would look like."""
    print(f"\n{'='*70}")
    print("📦 TRADITIONAL BATCH PROCESSING (for comparison):")
    print("-" * 70)
    print("   Traditional approach: Start all tasks → Wait for ALL → Yield ALL at once")
    print()
    
    # Simulate traditional gather approach
    traditional_start = time.time()
    
    # All tasks start simultaneously (like our streaming approach)
    print("🔄 All tasks starting simultaneously...")
    for task_name, processing_time in PROCESSING_TASKS:
        print(f"   → '{task_name}' ({processing_time}s)")
    
    # Wait for the longest task (simulating gather behavior)
    max_time = max(time_val for _, time_val in PROCESSING_TASKS)
    await asyncio.sleep(max_time)
    
    traditional_total = time.time() - traditional_start
    
    print(f"\n📦 ALL {len(PROCESSING_TASKS)} results available at {traditional_total:.3f}s")
    print("   → No results available until ALL tasks complete")
    print("   → Subsequent pipeline stages must wait for slowest task")
    print("   → Poor user experience (no progress indication)")
    
    return traditional_total

async def main():
    """Run the complete demonstration."""
    print("This example demonstrates Conveyor's streaming performance benefits")
    print("using an ordered queue approach that yields results as early as possible")
    print("while preserving input order.\n")
    
    # Main streaming demonstration
    streaming_results = await demonstrate_streaming_benefits()
    
    # Traditional comparison
    traditional_time = await demonstrate_traditional_comparison()
    
    # Final summary
    print(f"\n{'='*70}")
    print("💡 KEY TAKEAWAYS:")
    print("="*70)
    print("1. 🚀 Streaming yields results immediately when possible")
    print("2. 🔒 Order preservation is maintained (slow items block subsequent items)")
    print("3. ⚡ Early results enable immediate downstream processing")
    print("4. 📈 Better resource utilization across entire pipeline")
    print("5. 🎯 User experience: Progressive results vs all-at-once")
    print()
    
    print(f"In this demonstration:")
    print(f"• Streaming gave us the 1st result {streaming_results['improvement_1st']:.0f}% faster")
    print(f"• Streaming gave us the first 5 results {streaming_results['improvement_5th']:.0f}% faster")
    print(f"• The slow outlier properly blocked remaining results to preserve order")
    print(f"• Once the outlier completed, buffered results were yielded immediately")
    
    return streaming_results

if __name__ == "__main__":
    asyncio.run(main())
import asyncio
import time
from typing import AsyncIterable
from conveyor import single_task, Pipeline, PipelineContext

# Sample text with lines of varying processing times
SAMPLE_DATA = [
    ("Short task", 0.1),
    ("Medium length task here", 0.3), 
    ("Very long task that takes a significant amount of time to process", 0.8),
    ("Quick", 0.05),
    ("Another medium task", 0.25),
    ("Extremely long task that will definitely take the longest time", 1.0),
    ("Fast", 0.02),
    ("Normal task", 0.2),
]

@single_task
async def process_with_delay(item: tuple[str, float]) -> dict:
    """Process each item with a delay proportional to its complexity."""
    text, delay = item
    start_time = time.time()
    
    print(f"🔄 Starting: '{text}' (expected {delay}s)")
    await asyncio.sleep(delay)
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    result = {
        'text': text,
        'expected_delay': delay,
        'actual_time': round(processing_time, 3),
        'completed_at': time.time()
    }
    
    print(f"✅ Completed: '{text}' in {processing_time:.3f}s")
    return result

@single_task
async def add_metadata(result: dict) -> dict:
    """Add some metadata to show pipeline context usage."""
    from conveyor import get_current_context

    context = get_current_context()
    result["pipeline_info"] = {
        "pipeline_id": (
            context.pipeline_id[:8] if context and context.pipeline_id else "unknown"
        ),
        "execution_mode": context.execution_mode if context else "unknown",
    }

    # Add some custom context data
    if context:
        context.data['processed_count'] = context.data.get('processed_count', 0) + 1
        result['pipeline_info']['processed_count'] = context.data['processed_count']

    return result

async def main():
    print("=== Pipeline Execution Mode Comparison ===\n")

    # Create the pipeline
    pipeline = process_with_delay | add_metadata

    print("🔍 Input data (text, expected_delay):")
    for i, (text, delay) in enumerate(SAMPLE_DATA, 1):
        print(f"  {i}. '{text}' ({delay}s)")
    print()

    # Test 1: Ordered execution (default)
    print("📋 TEST 1: ORDERED EXECUTION (preserves input order)")
    print("-" * 60)

    start_time = time.time()
    ordered_results = []

    async for result in pipeline(SAMPLE_DATA):
        ordered_results.append(result)
        print(f"📤 Received: '{result['text']}' | Pipeline: {result['pipeline_info']['pipeline_id']} | Mode: {result['pipeline_info']['execution_mode']}")

    ordered_time = time.time() - start_time
    print(f"\n⏱️  Total ordered execution time: {ordered_time:.3f}s")

    # Test 2: As-completed execution using pipeline.as_completed()
    print(f"\n📋 TEST 2: AS_COMPLETED EXECUTION (completion order)")
    print("-" * 60)

    start_time = time.time()
    completed_results = []

    async for result in pipeline.as_completed(SAMPLE_DATA):
        completed_results.append(result)
        print(f"🚀 Received: '{result['text']}' | Pipeline: {result['pipeline_info']['pipeline_id']} | Mode: {result['pipeline_info']['execution_mode']}")

    completed_time = time.time() - start_time
    print(f"\n⏱️  Total as-completed execution time: {completed_time:.3f}s")

    # Test 3: Using stream.as_completed() to collect all at once
    print(f"\n📋 TEST 3: STREAM.AS_COMPLETED() (collect all in completion order)")
    print("-" * 60)

    start_time = time.time()

    stream = pipeline.with_execution_mode("as_completed")(SAMPLE_DATA)
    stream_results = []
    async for result in stream.as_completed():
        stream_results.append(result)

    stream_time = time.time() - start_time
    print(f"⚡ Collected {len(stream_results)} results in completion order")
    for result in stream_results:
        print(f"   '{result['text']}' | Pipeline: {result['pipeline_info']['pipeline_id']} | Mode: {result['pipeline_info']['execution_mode']}")
    print(f"\n⏱️  Total stream as-completed time: {stream_time:.3f}s")

    # Test 4: Custom context usage
    print(f"\n📋 TEST 4: CUSTOM CONTEXT (pipeline-wide data sharing)")
    print("-" * 60)

    # Create a custom context with shared data
    custom_context = PipelineContext(
        execution_mode="as_completed",
        data={'custom_tag': 'performance_test', 'start_time': time.time()}
    )

    custom_pipeline = pipeline.with_context(**custom_context.__dict__)
    context_results = []

    async for result in custom_pipeline(SAMPLE_DATA[:4]):  # Use fewer items for demo
        context_results.append(result)
        print(f"🎯 Received: '{result['text']}' | Count: {result['pipeline_info']['processed_count']}")

    # Analysis
    print(f"\n📊 EXECUTION ANALYSIS:")
    print("-" * 60)

    print(f"1. Ordered execution:     {ordered_time:.3f}s")
    print(f"2. As-completed execution: {completed_time:.3f}s")  
    print(f"3. Stream as-completed:   {stream_time:.3f}s")

    performance_improvement = ((ordered_time - completed_time) / ordered_time) * 100
    print(f"\n🚀 Performance improvement with as_completed: {performance_improvement:.1f}%")

    # Verify order differences
    print(f"\n📝 ORDER COMPARISON:")
    print("Original order:", [item[0][:20] + "..." if len(item[0]) > 20 else item[0] for item in SAMPLE_DATA])
    print("Ordered results:", [r['text'][:20] + "..." if len(r['text']) > 20 else r['text'] for r in ordered_results])
    print("As-completed results:", [r['text'][:20] + "..." if len(r['text']) > 20 else r['text'] for r in completed_results])

    # Context data demonstration
    if context_results:
        print(f"\n🎯 CONTEXT DATA DEMO:")
        last_result = context_results[-1]
        print(f"Pipeline processed {last_result['pipeline_info']['processed_count']} items")
        print(f"Pipeline ID: {last_result['pipeline_info']['pipeline_id']}")

    return {
        'ordered_results': ordered_results,
        'completed_results': completed_results,
        'stream_results': stream_results,
        'context_results': context_results,
        'ordered_time': ordered_time,
        'completed_time': completed_time,
        'stream_time': stream_time,
        'performance_improvement': performance_improvement
    }

if __name__ == "__main__":
    asyncio.run(main())

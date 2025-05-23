import asyncio
import time
from typing import AsyncIterable, Iterable
from conveyor import single_task, Pipeline

# Sample text with lines of varying lengths to test processing order
SAMPLE_TEXT = """Short line.
This is a medium length line with more words.
Very long line with many many words that will take significantly more time to process due to its length.
Tiny.
Another medium-sized line here.
Extremely long line that contains a lot of text and will definitely take the longest time to process because of all these extra words.
Quick.
Medium line again.
Final short."""

@single_task
async def split_by_lines(text: str) -> list[str]:
    """Split text into individual lines"""
    lines = text.strip().split('\n')
    return lines

@single_task
async def split_by_lines_iter(text: str) -> AsyncIterable[str]:
    """Split text into individual lines"""
    lines = text.strip().split('\n')
    for line in lines:
        yield line.strip()

@single_task
async def process_line_with_length_delay(line: str) -> dict:
    """Process each line by calculating its length and sleeping proportionally"""
    start_time = time.time()
    line_length = len(line)
    
    print(f"Starting to process: '{line}' (length: {line_length})")
    
    # Sleep for a duration proportional to the line length
    # Shorter lines finish faster, longer lines take more time
    sleep_duration = line_length * 0.05  # 50ms per character
    await asyncio.sleep(sleep_duration)
    
    end_time = time.time()
    processing_time = end_time - start_time
    
    result = {
        'line': line,
        'length': line_length,
        'processing_time': round(processing_time, 3),
        'sleep_duration': round(sleep_duration, 3)
    }
    
    print(f"Completed: '{line}' - Length: {line_length}, Time: {processing_time:.3f}s")
    return result

async def main():
    print("=== Split and Process Example: Testing Order Preservation ===\n")
    print("This example processes lines with sleep time proportional to line length.")
    print("If results come back in order of completion (shortest first), order is NOT preserved.")
    print("If results come back in original line order, order IS preserved.\n")
    
    # Create the pipeline
    pipeline = split_by_lines | process_line_with_length_delay
    
    print("Input text:")
    print("-" * 50)
    print(SAMPLE_TEXT)
    print("-" * 50)
    print()
    
    # Test 1: Stream results as they come
    print("🔄 Processing results as a stream (shows completion order):")
    print("-" * 60)
    
    start_time = time.time()
    completion_order = []
    
    stream = pipeline(SAMPLE_TEXT)
    async for result in stream:
        completion_order.append(result['line'])
        print(f"📤 Received: '{result['line']}' (length: {result['length']}, took: {result['processing_time']}s)")
    
    total_stream_time = time.time() - start_time
    print(f"\nTotal streaming time: {total_stream_time:.3f}s")
    
    # Test 2: Collect all results
    print("\n" + "="*60)
    print("📦 Collecting all results (shows final order):")
    print("-" * 60)
    
    start_time = time.time()
    results = await pipeline(SAMPLE_TEXT).collect()
    total_collect_time = time.time() - start_time
    
    print("Final collected results:")
    for i, result in enumerate(results, 1):
        print(f"{i:2d}. '{result['line']}' (length: {result['length']}, processing_time: {result['processing_time']}s)")
    
    print(f"\nTotal collection time: {total_collect_time:.3f}s")
    
    # Analysis
    print("\n" + "="*60)
    print("📊 ORDER PRESERVATION ANALYSIS:")
    print("-" * 60)
    
    original_lines = [line.strip() for line in SAMPLE_TEXT.strip().split('\n')]
    collected_lines = [result['line'] for result in results]
    
    print(f"Original line order: {[f'Line {i+1}' for i in range(len(original_lines))]}")
    print(f"Completion order during streaming: {[f'Line {original_lines.index(line)+1}' for line in completion_order]}")
    print(f"Final collected order: {[f'Line {i+1}' for i in range(len(collected_lines))]}")
    
    # Check if order is preserved
    order_preserved = original_lines == collected_lines
    
    print(f"\n🎯 ORDER PRESERVED: {'✅ YES' if order_preserved else '❌ NO'}")
    
    if order_preserved:
        print("   → The pipeline maintains the original order of items in the final results")
        print("   → Items may complete out of order (as seen in streaming), but final results are ordered")
    else:
        print("   → The pipeline does NOT preserve the original order")
        print("   → Results are delivered in completion order")
    
    # Performance insights
    line_lengths = [len(line.strip()) for line in original_lines]
    processing_times = [result['processing_time'] for result in results]
    
    print(f"\n📈 PERFORMANCE INSIGHTS:")
    print(f"   → Shortest line: {min(line_lengths)} chars, fastest processing: {min(processing_times):.3f}s")
    print(f"   → Longest line: {max(line_lengths)} chars, slowest processing: {max(processing_times):.3f}s")
    print(f"   → Average processing time: {sum(processing_times)/len(processing_times):.3f}s")
    
    # Return data for testing
    return {
        'results': results,
        'completion_order': completion_order,
        'original_lines': original_lines,
        'collected_lines': collected_lines,
        'order_preserved': order_preserved
    }

if __name__ == "__main__":
    asyncio.run(main())
import pytest
import asyncio
import sys
import os

# Add the examples directory to sys.path to allow importing example modules
EXAMPLES_DIR = os.path.join(os.path.dirname(__file__), '..', 'examples')
sys.path.insert(0, EXAMPLES_DIR)
# Also add the project root to sys.path for conveyor_streaming imports if examples are run directly
PROJECT_ROOT_DIR = os.path.join(os.path.dirname(__file__), '..')
sys.path.insert(0, PROJECT_ROOT_DIR)

# Import the main functions from the example files
from examples.quick_start_example import main as quick_start_main
from examples.min_batch_size_example import main as min_batch_size_example_main
from examples.side_inputs_example import main_with_side_inputs
from examples.error_handling_readme_example import main_error_handling
from examples.split_and_process_example import main as split_and_process_main
from examples.batch_item_generating import generate_batches_with_delay, process_batch, add_timestamp

@pytest.mark.asyncio
async def test_quick_start_example():
    """Test the quick_start_example.py output."""
    results = await quick_start_main()
    # Expected results based on README discussion and example file:
    # m(1)=2, m(2)=4, m(3)=6 -> sum_batch([2,4,6])=12 -> add_ten(12)=22
    # m(4)=8, m(5)=10, m(6)=12 -> sum_batch([8,10,12])=30 -> add_ten(30)=40
    # m(7)=14 -> sum_batch([14])=14 -> add_ten(14)=24
    # Final Expected: [22, 40, 24]
    assert results == [22, 40, 24]

@pytest.mark.asyncio
async def test_min_batch_size_example_main():
    """Test the quick_start_example.py output."""
    results = await min_batch_size_example_main()
    # Expected results based on README discussion and example file:
    # m(1)=2, m(2)=4, m(3)=6 -> sum_batch([2,4,6])=12 -> add_ten(12)=22
    # m(4)=8, m(5)=10, m(6)=12 -> sum_batch([8,10,12])=30 -> add_ten(30)=40
    # m(7)=14. Buffer for sum_batch is [14]. len is 1. min_size is 2. So it's not processed by sum_batch.
    # Final Expected: [22, 40]
    assert results == [22, 40]

@pytest.mark.asyncio
async def test_side_inputs_example():
    """Test the side_inputs_example.py outputs."""
    all_results = await main_with_side_inputs()

    # Expected for direct value: [(1*2 + 10)*2, (2*2 + 10)*2] = [24, 28]
    assert all_results["direct"] == [24, 28]

    # Expected for coroutine: [(1*2 + 100)*2, (2*2 + 100)*2] = [204, 208]
    assert all_results["coroutine"] == [204, 208]

    # Expected for AsyncStream: [(1*2 + 50)*2, (2*2 + 50)*2] = [104, 108]
    assert all_results["stream"] == [104, 108]

@pytest.mark.asyncio
async def test_error_handling_readme_example():
    """Test the error_handling_readme_example.py outputs."""
    all_results = await main_error_handling()

    # Results are non-deterministic due to random failures, but we can test structure
    assert "skip_items" in all_results
    assert "retry" in all_results  
    assert "custom_handler" in all_results

    # skip_items: Should have some results (negative batches are skipped)
    # The exact results depend on random failures, but should be a list
    assert isinstance(all_results["skip_items"], list)

    # retry: Should have some results after retries
    assert isinstance(all_results["retry"], list)

    # custom_handler: Should have results with -1 for multiples of 7
    # Input [1, 2, 7, 14, 21] -> [5, 10, -1, -1, -1] (business rule violations become -1)
    assert all_results["custom_handler"] == [5, 10, -1, -1, -1]


@pytest.mark.asyncio
async def test_split_and_process_example():
    """Test the split_and_process_example.py output."""
    results = await split_and_process_main()

    # Verify the structure of returned data
    assert isinstance(results, dict)
    assert "results" in results
    assert "completion_order" in results
    assert "original_lines" in results
    assert "collected_lines" in results
    assert "order_preserved" in results

    # Check that we have results for all 9 lines in SAMPLE_TEXT
    assert len(results["results"]) == 9
    assert len(results["original_lines"]) == 9
    assert len(results["collected_lines"]) == 9

    # Verify each result has the expected structure
    for result in results["results"]:
        assert isinstance(result, dict)
        assert "line" in result
        assert "length" in result
        assert "processing_time" in result
        assert "sleep_duration" in result
        assert isinstance(result["line"], str)
        assert isinstance(result["length"], int)
        assert isinstance(result["processing_time"], float)
        assert isinstance(result["sleep_duration"], float)

    # Verify that the original lines match the collected lines (order preservation)
    assert results["original_lines"] == results["collected_lines"]
    assert results["order_preserved"] == True

    # Verify that line lengths match expected values
    expected_lines = [
        "Short line.",
        "This is a medium length line with more words.",
        "Very long line with many many words that will take significantly more time to process due to its length.",
        "Tiny.",
        "Another medium-sized line here.",
        "Extremely long line that contains a lot of text and will definitely take the longest time to process because of all these extra words.",
        "Quick.",
        "Medium line again.",
        "Final short.",
    ]

    for i, result in enumerate(results["results"]):
        assert result["line"] == expected_lines[i]
        assert result["length"] == len(expected_lines[i])
        # Sleep duration should be length * 0.05
        assert abs(result["sleep_duration"] - (len(expected_lines[i]) * 0.05)) < 0.001

@pytest.mark.asyncio
async def test_batch_item_generating():
    """Test the zero-input pipeline with batch processing example."""
    # Create a pipeline starting with our source generator
    from conveyor import Pipeline
    
    # Create a pipeline that starts with a source generator
    pipeline = generate_batches_with_delay | process_batch | add_timestamp
    
    # Collect all results
    all_results = []
    batch_groups = {}
    
    # Execute the pipeline without providing any input data
    # since our source generator doesn't require any
    async for result in pipeline():
        all_results.append(result)
        
        # Track which batch this item was processed in
        batch_key = str(result["processed_with"])
        if batch_key not in batch_groups:
            batch_groups[batch_key] = []
        batch_groups[batch_key].append(result["id"])
    
    # Verify we got all expected results
    assert len(all_results) == 15, "Should have 15 total results (3 batches of 5 items)"
    
    # Verify that all items have the required metadata
    for result in all_results:
        assert "id" in result, "Each result should have an id"
        assert "batch" in result, "Each result should have a batch number"
        assert "processed" in result, "Each result should have processed flag"
        assert "processed_with" in result, "Each result should track which items it was processed with"
        assert "processed_at" in result, "Each result should have a processed timestamp"
        assert "emerged_at" in result, "Each result should have an emerged timestamp"
        assert "total_time" in result, "Each result should have total processing time"
    
    # Verify batch processing logic
    assert len(batch_groups) >= 5, "Should have at least 5 processing batches with max_size=3"
    
    # Verify item order preservation and batch boundaries
    # Items should be received in the original order, even when processed in different batches
    ids = [result["id"] for result in all_results]
    expected_ids = [
        # First source batch (5 items)
        0, 1, 2, 3, 4,
        # Second source batch (5 items)
        10, 11, 12, 13, 14,
        # Third source batch (5 items)
        20, 21, 22, 23, 24
    ]
    assert ids == expected_ids, "Results should preserve original order of items across batches"
    
    # Check timing patterns by comparing emergence times instead of processing times
    # Items from later batches should emerge later than items from earlier batches
    first_batch_emerged_at = all_results[0]["emerged_at"]
    last_batch_emerged_at = all_results[-1]["emerged_at"]
    assert last_batch_emerged_at > first_batch_emerged_at, "Later batches should emerge later due to source delays"

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


import asyncio
from conveyor import single_task, batch_task, Pipeline

# Define some tasks
@single_task
async def multiply_by_two(x: int) -> int:
    print(f"Multiplying {x} by 2")
    await asyncio.sleep(0.01) # simulate io-bound work
    return x * 2

@batch_task(max_size=3)
async def sum_batch(batch: list[int]) -> int:
    print(f"Summing batch: {batch}")
    await asyncio.sleep(0.05) # simulate io-bound work
    s = sum(batch)
    print(f"Sum of batch {batch} is {s}")
    return s

@single_task
async def add_ten(x: int) -> int:
    print(f"Adding 10 to {x}")
    await asyncio.sleep(0.01)
    return x + 10

async def main():
    # Create a pipeline
    pipeline = multiply_by_two | sum_batch | add_ten

    data_source = [1, 2, 3, 4, 5, 6, 7]
    
    # Get the async stream
    stream = pipeline(data_source)

    print("Collecting all results...")
    results=[]
    async for result in stream:
        results.append(result)
        print(f"Streamed result: {result}")
    
    # Expected results:
    # m(1)=2, m(2)=4, m(3)=6 -> sum_batch([2,4,6])=12 -> add_ten(12)=22
    # m(4)=8, m(5)=10, m(6)=12 -> sum_batch([8,10,12])=30 -> add_ten(30)=40
    # m(7)=14 -> sum_batch([14])=14 -> add_ten(14)=24
    # Final Expected: [22, 40, 24]
    return results

if __name__ == "__main__":
    asyncio.run(main())

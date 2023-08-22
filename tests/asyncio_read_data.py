from src import azure_api as az
import asyncio
import time
import pandas as pd


async def read_df(filename):
    blob_handler = az.OpBlobHandler()
    df = blob_handler.read_csv("op-test-csv", filename)

    return df


async def read_all():
    numbers = list(range(64))
    tasks = []
    for number in numbers:
        filename = f"20230809_{number}.csv"
        task = asyncio.create_task(read_df(filename))
        tasks.append(task)

    all_tasks = await asyncio.gather(*tasks)
    return all_tasks


async def main():
    df = [await read_all()]

    for _d in df:
        dfs = pd.concat(_d, ignore_index=True)

    print(len(dfs))


start = time.time()
asyncio.run(main())
end = time.time()
print(end - start)

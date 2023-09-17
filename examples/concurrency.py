import asyncio
import functools
from task_runner import execute


async def pause(seconds: int):
    if seconds > 5:
        raise Exception('Can\'t sleep for too long.')
    
    await asyncio.sleep(seconds)
    print(f'Slept for {seconds} seconds.')
    return seconds

if __name__ == '__main__':
    tasks=[
        functools.partial(pause, 1),
        functools.partial(pause, 1),
        functools.partial(pause, 10),
        functools.partial(pause, 1),
        functools.partial(pause, 2),
        functools.partial(pause, 1)
    ]

    results = asyncio.run(
        execute(
            tasks=tasks,
            tasks_limit=4,
            retry_failures=True,
            retry_timeout=2,
            retry_limit=2
        )
    )

    print(results)

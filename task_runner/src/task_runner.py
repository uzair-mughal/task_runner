import asyncio
import logging
from collections import OrderedDict
from typing import Coroutine, List
from task_runner.src.exceptions import InvalidRetryLimitException
from task_runner.src.exceptions import InvalidRetryTimeoutException


logger = logging.getLogger(__name__)


async def execute(
    tasks: List[Coroutine],
    tasks_limit: int = 10,
    retry_failures: bool = False,
    retry_limit: int = 1,
    retry_timeout: int = 10
):
    '''
    Executes the given tasks in parallel.

    ----------
    Parameters
    ----------

    tasks:              List of tasks to be executed in parallel.
    tasks_limit:        Number to tasks to be execute in parallel.
    retry_failures:     Flag to retry failed tasks.
    retry_limit:        Number of retries for failed tasks.
    retry_timeout:      Number of seconds to delay between each retry.

    -------
    Returns
    -------

    list[tuple]:        List of touple containing taks names mapped with their results.
    '''

    # Validate retry_limit parameter.
    if retry_failures and retry_limit < 1:
        raise InvalidRetryLimitException()

    # Validate retry_timeout parameter.
    if retry_failures and retry_timeout < 0:
        raise InvalidRetryTimeoutException()

    # Adjust tasks_limit for semaphore.
    if tasks_limit < 1:
        tasks_limit = 1

    semaphore = asyncio.Semaphore(tasks_limit)

    # Closure function.
    # Wraps a coroutine to use semaphore.
    async def execute_with_semaphore(task):
        async with semaphore:
            return await task

    new_tasks = OrderedDict()
    for task in tasks:
        new_task = asyncio.ensure_future(execute_with_semaphore(task()))
        new_tasks[new_task.get_name()] = (new_task, task)

    results = dict()
    pending_tasks = set(value[0] for value in new_tasks.values())
    retries = 0

    while pending_tasks:
        executed_tasks, pending_tasks = await asyncio.wait(
            pending_tasks,
            return_when=asyncio.ALL_COMPLETED
        )

        for task in executed_tasks:
            if task.exception():
                logger.info(f'Exception raised in {new_tasks[task.get_name()][1]}.')
                logger.error(task.exception())

                results[task.get_name()] = task.exception()

                if retry_failures and retries < retry_limit:
                    # If retry_failures is enabled then schedule failed tasks for retry.
                    # Making sure that the failed task's key value is replaced by the
                    # newly created task.

                    new_tasks[task.get_name()] = (
                        asyncio.ensure_future(execute_with_semaphore(new_tasks[task.get_name()][1]())),
                        new_tasks[task.get_name()][1]
                    )

                    new_tasks[task.get_name()][0].set_name(task.get_name())
                    pending_tasks.add(new_tasks[task.get_name()][0])

                    logger.info(f'{new_tasks[task.get_name()][1]} scheduled for restart.')

            else:
                results[task.get_name()] = task.result()

        if pending_tasks and retries < retry_limit:
            # Schedule retry after timeout.
            await asyncio.sleep(retry_timeout)
            retries += 1

    return list(dict(sorted(results.items())).values())

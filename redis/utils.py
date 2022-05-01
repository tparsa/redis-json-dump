from typing import Iterable


def to_batch(itr: Iterable, batch_size: int) -> Iterable[list]:
    batch = []
    for item in itr:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if len(batch) > 0:
        yield batch

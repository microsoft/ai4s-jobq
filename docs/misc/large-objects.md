# Large objects as task parameters

If tasks are too large to fit in the queue, you can "stash" the data away.

```python
class LargeBatchProcessor(WorkSpecification):
  def __init__(self):
    super().__init__()
    self.stashblobs = BlobContainer(storage_account="mystorageaccount", container="stash-container")
    self.register_context_manager(self.stashblobs)

  async def list_tasks(self, seed, force=False):
    async for large_batch_object in some_generator():
      yield dict(batch=large_batch_object)

  async def enqueue_task(self, batch):
    # Stash the large object in a blob container
    stash = await self.stashblobs.stash_as_pickle(batch)
    return dict(filename=stash.filename, md5sum=stash.md5sum)

  async def __call__(self, filename, md5sum, **kwargs):
    # Retrieve the large object from the stash
    batch = await self.stashblobs.unstash_from_pickle(filename, md5sum)
```

Ideally, set a retention policy for the stash-container, so that your stash
blobs are automatically cleaned up after a certain period.

# Fs2 queue based streams "quirks"

- A normal stream description `Stream.emits(...)` (RF) thus cannot be replaced by a queue based stream `queue.dequeue` (not RF) 
- head/tail/take ... can have surprising semantics on queue based streams 

In summary, queue based streams are not referentially transparent which is to be expected...
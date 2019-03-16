# Fs2 queue based streams "quirks"


A normal stream description `Stream.emits(...)` (RF) cannot be replaced by a queue based stream `queue.dequeue` (not RF) 


Issues to consider with queue (non-RF) based streams
- Consumption and production might be might be temporally coupled depending on the queue backpressure strategy
 - Unbounded/no backpressure -> no coupling (no backpressure = danger oO !)  
 - Bounded/blocking backpressure -> no coupling 
 - Bounded/dropping backpressure -> coupled, elements might get lost depending on enqueue/dequeue timing
 



In summary, queue based streams are not referentially transparent which is to be expected...

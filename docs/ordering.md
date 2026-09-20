# Ordering and fairness

Within one queue, jobs are claimed in this order:

1. `priority` descending (a small integer, default 0).
2. `scheduled_at` ascending, considering only jobs whose time has come.
3. Insertion order, which is the identity `seq` column and not a timestamp.

This is the `ORDER BY` of the `claim_candidates` statement in
[`src/rqueue/storage.py`](../src/rqueue/storage.py).

**Between queues there is no fairness mechanism.** A `Worker` serves exactly one
queue. Run one worker per queue and size each deliberately. A busy queue cannot
starve a quiet one because they do not share a worker.

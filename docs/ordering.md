# Ordering

Within one queue, jobs are claimed in this order:

1. `priority`, highest first. It is a small integer and defaults to 0.
2. `scheduled_at`, earliest first, among jobs whose time has come.
3. Insertion order. This is the identity column `seq`, not a timestamp.

This is the `ORDER BY` of the `claim_candidates` statement in
[`src/rqueue/storage.py`](../src/rqueue/storage.py).

Between queues there is no ordering and no fairness mechanism. A `Worker` serves
one queue, so a busy queue cannot starve a quiet one. Run one worker per queue
and size each for its load.

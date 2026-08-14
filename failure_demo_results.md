## Broker failure recovery — measured

Broker stopped for 181s under a running job.

| | before | after restart | after recovery |
|---|---|---|---|
| silver rows | 4,486 | 4,486 | 5,865 |
| bronze rows | 110,563 | 110,563 | 156,000 |
| duplicate keys | 0 | 0 | 0 |

The streaming job did not exit during the outage. On restart it resumed from checkpointed offsets and drained the backlog in ~2278s, recovering 1,379 rows with 0 duplicate keys.

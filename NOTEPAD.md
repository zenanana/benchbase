# Week of Apr 24 2023
## Goodput
Code for calculation of goodput and throughput below:
```
public double requestsPerSecondThroughput() {
    return (double) measuredRequests / (double) nanoseconds * 1e9;
}

public double requestsPerSecondGoodput() {
    return (double) success.getSampleCount() / (double) nanoseconds * 1e9;
}
```
Looks like goodput is calculated purely from successful/completed transactions (does not include aborted/rejected trx)
<br/>

Issue that introduces goodput is [here](https://github.com/cmu-db/benchbase/pull/102)

## Aborted vs Rejected Transactions (Server Retry)
Rejected trx seems to be equivalent to retried trx where the trx did not execute.
<br/>

Whereas aborted trx has executed successfully but was aborted due to valid user control code (not sure in what cases this happends).
<br/>

See `TransactionStatus.java` code snippet below:
```
/**
* The transaction executed successfully but then was aborted
* due to the valid user control code.
* This is not an error.
*/
USER_ABORTED,
/**
* The transaction did not execute due to internal
* benchmark state. It should be retried
*/
RETRY,

/**
* The transaction did not execute due to internal
* benchmark state. The Worker should retry but select
* a new random transaction to execute.
*/
RETRY_DIFFERENT,
```
And rejected is the sum of both types of retries. See `DBWorkload.java`:
```
map.put("rejected", r.getRetry());
```
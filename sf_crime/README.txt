Question 1
How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

It changes processedRowsPerSecond.
 
Question 2
What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

I didn't have any issue with running what I originally had, but to determine optimal variation we can consider the following parameters:
parallelism, shuffle.partitions, maxOffsetsPerTrigger, maxRatePerPartition, etc
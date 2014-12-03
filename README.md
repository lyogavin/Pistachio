Pistachio
==========
The biggest cost of distributed computing is the disk and network IO overhead. Data locality is often key to the performance of distributed computing. The existing commonly used systems like Hadoop and Storm are not designed for improving data locality, so they have bottlenecks to achieve the best performance. Also memeory and SSD drives become cheaper and cheaper. Keeping data in memory or SSD reliably can often significantly improve performance of distributed compute. Pistachio is designed for addressing those problems to make distributed computing 10x faster.

Pistachio is originally designed for low latency, strong consistency user data storage in ad serving in Yahoo. In our use case, it stores data for billions of users, supporting millions of reads and writes QPS with very low latency. It guarantees strong consistency and can achieve great fault-tolerance, availability and scalability. We then extended it to do distribted compute to leverage the low latency. Previously we used thousands of Hadoop servers to process 10s TB data daily in 4 hours. With Pistachio, we can achieve realtime streaming processing with around hundred servers.

A solid and reliable storage layer is critical for distributed compute system. Like in Hadoop's case, the stability of HDFS is the foundation of Map Reduce's succeess. Lack of a stable and efficient enough storage layer is also often the pain point of using storm. Pistachio is firstly a succeessful storage system, then the distributed compute is all designed around Pistachio.

Pistachio colocates the compute and data as much as possible thus can avoid data transfer round trips and gaurantee the best data locality. If the memory is big enough, it can reliably store all the data in memory to reduce the latency.

http://lyogavin.github.io/Pistachio/

Copyright 2014 Yahoo! Inc. Licensed under the Apache License 2.0


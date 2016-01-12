This script will be used for fast rebalancing of data at OpenStack Swift
Based on old and new object ring files. 

This script assumes:
1. ssh keys distributed across the cluster
2. the drivers are mapped at the same way on all the machines
3. currently it only distributes the partition to the right disks,
   without the last step of moving it to the object directory.
# Simple Recovery Test

Modified from simpleops, the makefile includes build&test

```bash
make clean
make nondebug
make alloc NODE=0 OBJSIZE=192 NUMOBJ=4194304
make recovery NODE=0 OBJSIZE=192 NUMOBJ=4194304 NUMTHREAD=1
```

# recovery bench

FYI: Pangolin's new obj is not RAID-like logic, pay attention to the matching between poolsize and workload size, otherwise, many spaces will be empty.
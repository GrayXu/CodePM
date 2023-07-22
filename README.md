## CodePM: Parity-based Crash Consistency for Log-Free Persistent Transactional Memory

The repository includes the source code of CodePM in our paper accepted by IEEE TCAD 2024.

## Setup

Clone this project. You can switch between different git branches to view the diff.

```bash
git clone git@github.com:GrayXu/CodePM.git
cd CodePM
git submodule update --init --recursive
```

Prepare the pmem device.

```bash
# we use fsdax mode for pmem
sudo ndctl create-namespace --force --mode=fsdax --reconfig=namespace0.0
sudo mkfs.ext4 /dev/pmem0
sudo mount -o dax /dev/pmem0 /mnt/pmem0 
```

## Build

All dependencies are consistent with PMDK v1.5. Please refer to the [PMDK doc](https://github.com/GrayXu/CodePM/blob/main/README_pmdk.md) for details.

1. Build ISA-L-CodePM.

```bash
cd third_party/isa-l-pm
./autogen.sh
./configure
make
sudo make install
```

2. Build CodePM.

```bash
cd src
./runmake -mlpc --logfree --avx512code --pipeline --pipeline_data --pipeline_data_2m
```

The `runmake` script uses compile-time macros to enable or disable different components and completes relevant settings, including benchmark configurations.

```
Usage: ./runmake [-w|--watch <arg>] [--(no-)clang] [--(no-)probe] [-m|--(no-)metarep] [-l|--(no-)logrep] [-p|--(no-)parity] [-c|--(no-)checksum] [-e|--(no-)rclockhle] [-h|--help] [<target>]
        <target>: make target (default, clobber, check, test) (default: 'default')
        -w,--watch: watch build status in tmux (e.g. watch:0.1) (no default)                                                --clang,--no-clang: use clang and clang++ as compilers (off by default)
        --probe,--no-probe: enable probes for profiling (off by default)
        -m,--metarep,--no-metarep: enable pool's metadata replication (off by default)
        -l,--logrep,--no-logrep: enable transaction logs replication (off by default)
        -p,--parity,--no-parity: enable zone parity for object data redundancy (off by default)
        -c,--checksum,--no-checksum: enable pool's metadata and per-object checksums (off by default)
        -e,--rclockhle,--no-rclockhle: enable hardware lock elision for range column locks (off by default)
        --logfree: log-free (off by default)                                                                                --logfree-nofence: logfree and no inside fence  (off by default)
        --pipeline: early flush in the pipeline strategy (off by default)
        --pipeline_data: lazy fence after delta data in the pipeline strategy (off by default)
        --pipeline_data_2m: lazy fence after delta parity in the pipeline strategy (off by default)
        --avx512code: partially avx512 coding  (off by default)
        --force_atomic_xor: force atomic 8B xor (off by default)
        -h,--help: Prints help
```

## Run benchmarks

### pmembench (microbenchmarks)

- Under `benchmark/cfg` are the configurations for various types of benchmarks.
  - By specifying a config, `pmembench` runs the corresponding benchmark.
  - `ops_\*.cfg`: basic ops
  - `hashmap_\*.cfg`：map_insert on hashmap_tx
  - `trees_\*.cfg`：map_insert on ctree,rbtree,btree,skiplist,rtree
  - `pmembench_map.cfg`：map_insert on ctree,btree,rtree,rbtree,skiplist,hashmap_tx
- `pobjpool.set` contains settings for the location of pmempool. It needs to be specified in the benchmark config.
- The `runbench` script controls parameters, calls `pmembench` for testing, and outputs results, etc.


```bash
cd src/benchmarks
sudo ./runbench --report --avx512f --cpunodebind=0 --zone-rows=100 --zone-rows-parity=1 cfg/ops_write.cfg # run ops write bench
```

```bash
./runbench -h
Run a benchmark and report results
Usage: ./runbench [-t|--ntth <arg>] [-m|--message <arg>] [--(no-)pmem] [--(no-)clflushopt] [--(no-)clwb] [--(no-)movnt] [--(no-)avx] [--(no-)avx512f] [-i|--interleave <arg>] [-N|--cpunodebind <arg>] [-d|--(no-)gdb] [-r|--(no-)report] [-a|--(no-)append] [-h|--help] <cfg>
        <cfg>: benchmark configuration file (*.cfg)
        -t,--ntth: threshold for movnt-based memcpy (default: '256')
        -m,--message: note message for this run (no default)
        --pmem,--no-pmem: treat target file in configuration on pmem device (on by default)
        --clflushopt,--no-clflushopt: use clflushopt (if available) for persistence (on by default)
        --clwb,--no-clwb: use clwb (if available) for persistence (on by default)
        --movnt,--no-movnt: use movnt (sse2/avx/avx512f) for persistence (on by default)
        --avx,--no-avx: use avx for non-temporal memcpy to pmem (on by default)
        --avx512f,--no-avx512f: use avx512f for non-temporal memcpy to pmem (off by default)
        -i,--interleave: set a memory interleave policy; same argument for numactl (default: 'all')
        -N,--cpunodebind: only execute command on the CPUs of nodes; same argument for numactl (default: '0')
        --zone-rows: the number of rows (default: '10', only support 10~100)
        --zone-rows-parity: the number of parity rows (default '1', only support 1~2)
        -d,--gdb,--no-gdb: run the benchmark with gdb (off by default)
        -r,--report,--no-report: report results of interest (off by default)
        -a,--append,--no-append: append report to <bench>.rpt (off by default)
        -h,--help: Prints help
```

Using `-N0` specifies that the benchmark runs on the 0th CPU. In `cfg/ops_write.cfg`, the `pobjpool.set` will specify the path of pobjpool to be `/mnt/pmem0`.


### ycsb (macrobenchmarks)

First, generate the workloads of ycsb.

```bash
cd third_party/index-microbench
./generate_workloads.sh
cd ../../src/benchmarks
ln -s ../../third_party/index-microbench/workloads workloads
```

We developed `run_ycsb.cpp`, based on `pmembench.cpp`, to support basic YCSB workloads.
Therefore, the way to run `run_ycsb` is consistent with `pmembench`, but currently there is no `runbench` wrapper for ycsb, so you need to manually specify the parameters. An example is as follows:

```bash
sudo TCMALLOC_LARGE_ALLOC_REPORT_THRESHOLD=1000000000000 PMEM_AVX512F=1 PANGOLIN_ZONE_ROWS=${rows} PANGOLIN_ZONE_ROWS_PARITY=${parity_rows} numactl --cpubind=1 --membind=1 ./run_ycsb $objsize ${thread} pobjpool_1.set 1 "./workloads/load${workload}_unif_int.dat" "./workloads/txns${workload}_unif_int.dat"
```

## Crash recovery throughput

Check `src/examples/libpmemobj/simple_recovery/README.md` for details.

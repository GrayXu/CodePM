/*
 * run_ycsb.cpp -- a benchmark for libpmemobj based on YCSB workloads
 * ./run_ycsb [len] [num_thread] [poolset_path] [node_id] [ycsb_workload_load] [ycsb_workload_run]
 * @grayxu 2024
*/
// g++ -c -o run_ycsb.o -I../include -I../libpmemobj -I../common -I../examples/libpmemobj/map  run_ycsb.cpp

// g++ -o run_ycsb -L../debug  -L../examples/libpmemobj/map  -Wl,-rpath=../debug run_ycsb.o ../examples/libpmemobj/map/libmap.a  -lisal ../debug/libpmemcommon.a -lpmemobj -lpmemlog -lpmemblk -lpmempool -lpmem -lvmem -pthread -lm -lnuma -ldl  -lndctl -ldaxctl -lglib-2.0

// g++ -o run_ycsb -L../nondebug  -L../examples/libpmemobj/map  -Wl,-rpath=../nondebug run_ycsb.o ../examples/libpmemobj/map/libmap.a  -lisal ../nondebug/libpmemcommon.a -lpmemobj -lpmemlog -lpmemblk -lpmempool -lpmem -lvmem -pthread -lm -lnuma -ldl  -lndctl -ldaxctl -lglib-2.0

#include <immintrin.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <assert.h>
#include <unistd.h>
#include <time.h>

#include "os.h"
#include "os_thread.h"

#include <algorithm>
#include <iostream>
#include <unordered_map>
#include <queue>
#include <vector>

#include "x86intrin.h"
#include "libpangolin.h"

#include "map.h"
#include "map_btree.h"
#include "map_ctree.h"
#include "map_hashmap_atomic.h"
#include "map_hashmap_rp.h"
#include "map_hashmap_tx.h"
#include "map_rbtree.h"
#include "map_rtree.h"
#include "map_skiplist.h"

#define FILE_MODE 0666
#define LAYOUT_NAME "ycsb"

#define OP_READ 0
#define OP_UPDATE 1
#define OBJ_TYPE_NUM 1

#define MAX_HASHMAPS 18

TOID_DECLARE_ROOT(struct root);
struct root {
	// TOID(struct map) map;
	TOID(struct map) maps[MAX_HASHMAPS];
};

static const struct {
	const char *str;
	const struct map_ops *ops;
} map_types[] = {
	{"ctree", MAP_CTREE},		{"btree", MAP_BTREE},
	{"rtree", MAP_RTREE},		{"rbtree", MAP_RBTREE},
	{"hashmap_tx", MAP_HASHMAP_TX}, {"hashmap_atomic", MAP_HASHMAP_ATOMIC},
	{"hashmap_rp", MAP_HASHMAP_RP}, {"skiplist", MAP_SKIPLIST}};
#define MAP_TYPES_NUM (sizeof(map_types) / sizeof(map_types[0]))

PMEMobjpool *pop;

struct map_bench {
    struct map_ctx *mapc;
	PMEMobjpool *pop;
	TOID(struct root) root;
	PMEMoid root_oid;
	TOID(struct map) map;

	size_t nkeys;
	size_t init_nkeys;
	uint64_t *keys;
};

typedef struct __attribute__((aligned(4))) request_st {
	bool op; // read=0, update=1
	std::string key;
} req;

// param for each thread (in load or run stage)
typedef struct thread_kvs {
    int thread_id;
    int num_threads;
    PMEMobjpool *pop;
    struct map_bench* map_bench_worker;

    float run_time;
} thread_kvs;

int len_obj;
int num_threads;

std::vector<std::string> vload;	     // for loading
std::vector<struct request_st> vrun; // for running

/*
 * parse_map_type -- parse type of map
 */
static const struct map_ops *
parse_map_type(const char *str)
{
	for (unsigned i = 0; i < MAP_TYPES_NUM; i++) {
		if (strcmp(str, map_types[i].str) == 0)
			return map_types[i].ops;
	}

	return nullptr;
}

/*
 * malloc_aligned -- allocate an aligned chunk of memory filled with zeros
 */
static void *malloc_aligned(size_t size) {
    long pagesize = sysconf(_SC_PAGESIZE);  // default pagesize is 4096
    if (pagesize < 0) {
        perror("sysconf");
        return NULL;
    }

    /* allocate a page size aligned local memory pool */
    void *mem;
    int ret = posix_memalign(&mem, (size_t)pagesize, size);
    if (ret) {
        printf("posix_memalign failed\n");
        return NULL;
    }

    /* zero the allocated memory */
    memset(mem, 0, size);

    return mem;
}

double
timeval_diff(struct timespec *start, struct timespec *finish) {
	double total_t = (finish->tv_sec - start->tv_sec);
	total_t += (finish->tv_nsec - start->tv_nsec) / 1000000000.0;
	return total_t;
}

/**
 * load data of YCSB workloads into DRAM (`vload, vrun`)
 * @param load_fname the fname of ycsb_set.txt
 * @param run_fname the fname of ycsb_test.txt
 */
int init_trace(const char *load_fname, const char *run_fname) {
    printf("init_trace: %s, %s\n", load_fname, run_fname);
    FILE *fin_load = fopen(load_fname, "r");
    FILE *fin_run = fopen(run_fname, "r");
    if (fin_load == NULL || fin_run == NULL) {
        printf("%s, %s, open trace files failed\n",load_fname,run_fname);
        exit(1);
    }

    char tmp[1024];
    req r;
    // load ycsb_set.txt
    while (fgets(tmp, 1024, fin_load) && (!feof(fin_load))) {
        char key[250] = {0};
        if (sscanf(tmp, "INSERT %s", key)) {
            vload.push_back(std::string(key));
        }
    }
    long op_read_count = 0;
    long op_update_count = 0;

    // load ycsb_test.txt
    int not_count_size = 0;
    while (fgets(tmp, 1024, fin_run) && (!feof(fin_run))) {
        char key[250] = {0};
        if (sscanf(tmp, "READ %s", key)) {
            r.op = OP_READ;
            r.key = std::string(key);
            vrun.push_back(r);
            op_read_count++;
        } else if (sscanf(tmp, "UPDATE %s", key)) {
            r.op = OP_UPDATE;
            r.key = std::string(key);
            vrun.push_back(r);
            op_update_count++;
        } else {
            not_count_size++;
        }
    }

    fclose(fin_load);
    fclose(fin_run);

    printf("FILE LOAD OK: set size: %ld, test size: %ld, op_update_count: %ld, op_read_count: %ld\n", vload.size(),
           vrun.size(), op_update_count, op_read_count);
    return vrun.size();
}

/*
 * load workloads
*/
double init(char *argv[]) {
    // yscb-a as an example
    // init_trace("/home/gray/local-PM-EC/index-microbench/workloads/loada_unif_int.dat", 
    //            "/home/gray/local-PM-EC/index-microbench/workloads/txnsa_unif_int.dat");
    printf("init config and trace...\n");
    init_trace(argv[5], argv[6]);

    len_obj = atoi(argv[1]);                 // block size is len * 8 bits (char size)
    num_threads = atoi(argv[2]);             // the number of threads
    char *pobjpoolset_path = argv[3];  // dax path
    int node_id = atoi(argv[4]);             // the number of dimm

    // print config
    char * m_c = getenv("PANGOLIN_ZONE_ROWS");
    char * p_c = getenv("PANGOLIN_ZONE_ROWS_PARITY");
    int m = 10;
    int p = 1;
    if (m_c && p_c) {
	    m = strtol(m_c, NULL, 0);
	    p = strtol(p_c, NULL, 0);
    }
    int k = m - p;
    printf("len_obj=%d, num_threads=%d, pobj_path=%s, node_id=%d, k_ec=%d, p_ec=%d\n",
            len_obj, num_threads, pobjpoolset_path, node_id, k, p);

    // init pmemobj pool
    pop = pmemobj_create(pobjpoolset_path, LAYOUT_NAME, 0, 0666);
    if (pop == NULL) {
        fprintf(stderr, "pmemobj_create: %s\n", pmemobj_errormsg());
        exit(-6657);
    }
    return 0;
}

/*
 * load worker for each map
*/
void *worker_load(void *args_in) {
    int ret = 0;
    thread_kvs *args = (thread_kvs *)args_in;
    int thread_id = args->thread_id;

    // create map on root
    // struct map_bench* map_bench_worker = (struct map_bench *)calloc(1, sizeof(struct map_bench));
    // args->map_bench_worker = map_bench_worker;
    // map_bench_worker->pop = args->pop;
    // printf("\t\tthread_id: %d create_map\n", thread_id);
    // const struct map_ops *ops = parse_map_type("hashmap_tx");
    // struct map_ctx * mapc = map_ctx_init(ops, map_bench_worker->pop);
    // map_bench_worker->mapc = mapc;

    // map_bench_worker->root = POBJ_ROOT(map_bench_worker->pop, struct root);
    // if (TOID_IS_NULL(map_bench_worker->root)) exit(-6657);
    // map_bench_worker->root_oid = map_bench_worker->root.oid;

    // PGL_TX_BEGIN(map_bench_worker->pop)
	// {
	// 	struct root *proot = (struct root *)pgl_tx_open(map_bench_worker->root.oid);
	// 	// if (map_create(map_bench_worker->mapc, &proot->map, NULL)) exit(-6657);
	// 	if (map_create(map_bench_worker->mapc, &proot->maps[thread_id], NULL)) exit(-6657);
	// }
	// PGL_TX_END;

    // // map_bench_worker->map = D_RO(map_bench_worker->root)->map;
    // map_bench_worker->map = D_RO(map_bench_worker->root)->maps[thread_id];

    // create map outside
    struct map_bench* map_bench_worker = args->map_bench_worker;

    printf("\t\tthread_id: %d work_load\n", thread_id);
    /* load trace in this map*/
    size_t index = 0;
    for (std::string &key_v : vload) {
        uint64_t key; // 8B key
        memcpy(&key, key_v.c_str(), sizeof(uint64_t));
        // insert string as key to map_bench_worker->map
        PGL_TX_BEGIN(map_bench_worker->pop)
        {
            PMEMoid oid = pgl_tx_alloc(len_obj, OBJ_TYPE_NUM);  // as value
            ret = map_insert(map_bench_worker->mapc, map_bench_worker->map, key, oid);
            if (ret) exit(-6675);
        }
        PGL_TX_END;
        index++;
    }
    return NULL;
}

// worker run
void *worker_run(void *args_in) {
    int ret = 0;
    struct timespec start, finish;
    PMEMoid val = OID_NULL;

    thread_kvs * args = (thread_kvs *)args_in;
    int thread_id = args->thread_id;
    printf("\t\tthread_id: %d work_run\n", thread_id);
    struct map_bench* map_bench_worker = args->map_bench_worker;
    if (map_bench_worker == NULL) exit(-6657);
    char read_dest[4096];
    // printf("thread_id_%d starts...\n", thread_id);
    clock_gettime(CLOCK_MONOTONIC, &start);
    uint64_t index = 0;
    for (struct request_st &req : vrun) {
        if (req.op == OP_READ) {  // FOR READ
            uint64_t key;
            memcpy(&key, req.key.c_str(), sizeof(uint64_t));
            val = map_get(map_bench_worker->mapc, map_bench_worker->map, key);
            if (OID_IS_NULL(val)) {
                printf("wrong read!\n");
                exit(-6657);
                continue;
            }
            void *ptr = pgl_get(val);
            memcpy(read_dest, ptr, len_obj);
        } else {  // FOR UPDATE 
            uint64_t key;
            memcpy(&key, req.key.c_str(), sizeof(uint64_t));
            val = map_get(map_bench_worker->mapc, map_bench_worker->map, key);
            if (OID_IS_NULL(val)) {
                printf("\tNOT GET THE SPECIFIED VAL! by '%s'\n", req.key.c_str());
                exit(-6657);
                // continue;
            }
            PGL_TX_BEGIN(map_bench_worker->pop)
            {
                void *ptr = pgl_tx_open(val);
                pgl_tx_add_range(ptr, 0, len_obj);
                memset(ptr, 1, len_obj);  // update
            }
            PGL_TX_END;
        }
        index++;
    }

    clock_gettime(CLOCK_MONOTONIC, &finish);
    double total_t = timeval_diff(&start, &finish);
    args->run_time = total_t;

    return NULL;
}

int main(int argc, char *argv[]) {
    struct timespec start, finish;
    printf("-----run_ycsb.cpp------\n");
    printf("usage: ./run_ycsb [len] [num_thread] [poolset_path] [node_id] [ycsb_workload_load] [ycsb_workload_run]\n");

    if (argc < 7) {
        char *myargv[7];
        myargv[1] = "4032";           // len_obj
        myargv[2] = "4";             // num_thread
        myargv[3] = "pobjpool_1.set";  // pobjpool
        myargv[4] = "1";             // node_id
        myargv[5] = "workloads/loadu_unif_int.dat";
        myargv[6] = "workloads/txnsu_unif_int.dat";

        printf("\033[31mwrong parameters\033[0m, use default\n");
        init(myargv);
    } else {
        init(argv);
    }

    // assert(num_threads==1);  // not support multi-threads now

    // init worker threads
    pthread_t *threads = (pthread_t *)malloc_aligned(num_threads * sizeof(pthread_t));
    for (int i = 0; i < num_threads; i++) {
        pthread_t t = 0;
        threads[i] = t;
    }
    thread_kvs *args_array = (thread_kvs *)malloc_aligned(num_threads * sizeof(thread_kvs));

    printf("creating hashmaps...\n");
    /* serial create map for each worker outside */
    for (int i = 0; i < num_threads; i++) {
        int thread_id = i;
        thread_kvs *args = &args_array[i];
        args->thread_id = i;
        args->num_threads = num_threads;
        args->pop = pop;

        struct map_bench* map_bench_worker = (struct map_bench *)malloc_aligned(sizeof(struct map_bench));
        args->map_bench_worker = map_bench_worker;
        map_bench_worker->pop = pop;
        
        const struct map_ops *ops = parse_map_type("hashmap_tx");
        struct map_ctx * mapc = map_ctx_init(ops, map_bench_worker->pop);
        map_bench_worker->mapc = mapc;

        map_bench_worker->root = POBJ_ROOT(map_bench_worker->pop, struct root);
        if (TOID_IS_NULL(map_bench_worker->root)) exit(-6657);
        map_bench_worker->root_oid = map_bench_worker->root.oid;

        PGL_TX_BEGIN(map_bench_worker->pop)
        {
            struct root *proot = (struct root *)pgl_tx_open(map_bench_worker->root.oid);
            if (map_create(map_bench_worker->mapc, &proot->maps[thread_id], NULL)) exit(-6657);
        }
        PGL_TX_END;
        map_bench_worker->map = D_RO(map_bench_worker->root)->maps[thread_id];
    }

    printf("loading...\n");
    // /* start worker_load threads */ 
    // for (int i = 0; i < num_threads; i++) {
    //     thread_kvs *args = &args_array[i];
    //     args->thread_id = i;
    //     args->num_threads = num_threads;
    //     args->pop = pop;
    //     pthread_create(&threads[i], NULL, worker_load, args);
    // }
    // /* wait for loading */
    // for (int i = 0; i < num_threads; i++) pthread_join(threads[i], NULL);

    // serial loading data (but super slow for multi-threads and big objsize)
    for (int i = 0; i < num_threads; i++) {
        thread_kvs *args = &args_array[i];
        worker_load(args);  // do it serial
    }

    printf("running...\n");
    /* start worker_run threads */ 
    clock_gettime(CLOCK_MONOTONIC, &start);
    for (int i = 0; i < num_threads; i++) {
        thread_kvs *args = &args_array[i];
        pthread_create(&threads[i], NULL, worker_run, args);
    }

    /* wait for running */
    for (int i = 0; i < num_threads; i++) pthread_join(threads[i], NULL);
    clock_gettime(CLOCK_MONOTONIC, &finish);
    double total_t = timeval_diff(&start, &finish);
    printf("total time: %f\n", total_t);
    float result_sum_out =  vrun.size() / total_t;

    double total_run_iops = 0;

    pmemobj_close(pop);

    for (int i = 0; i < num_threads; i++) {
        double run_iops = vrun.size() / args_array[i].run_time;
        printf("\tthread %d: run_time: %f, run IOPS: %f\n", i, args_array[i].run_time, run_iops);
        total_run_iops += run_iops;
    }

    printf("sum_result: run IOPS= %f (%f)\n", result_sum_out, total_run_iops);
    

    return 0;
}
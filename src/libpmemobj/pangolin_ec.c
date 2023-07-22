/**
 * a simple C lang version of ec.hh in local-pm-ec to provide SIMD coding features
 */
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include <x86intrin.h>
#include <immintrin.h>

#include <isa-l.h>

#include "pangolin_ec.h"

static struct ec_runtime *ec;

/**
 * malloc aligned memory
*/
static void *
malloc_aligned(size_t size) {
    long pagesize = sysconf(_SC_PAGESIZE);  // default pagesize is 4096
    if (pagesize < 0) {
        return NULL;
    }

    /* allocate a page size aligned local memory pool */
    void *mem;
    int ret = posix_memalign(&mem, (size_t)pagesize, size);
    if (ret) {
        return NULL;
    }

    /* zero the allocated memory */
    memset(mem, 0, size);
    return mem;
}

int
init_ec_runtime(int k, int p, int len)
{
    assert(p<=2);  // TODO
    // printf("pangolin ec runtime: k=%d, p=%d, len=%d\n", k, p, len);
    ec = (struct ec_runtime *)malloc_aligned(sizeof(struct ec_runtime));
    ec->k = k;
    ec->p = p;
    // ec->len = len;
    ec->encode_matrix = (char *) malloc_aligned((k + p) * k);
    ec->g_tbls = (char *) malloc_aligned(k * p * 32);
    // ec->num_thread = 1;  // default
    
    gf_gen_rs_matrix(ec->encode_matrix, (k + p), k);  // fine by p<=3
    // gf_gen_cauchy1_matrix(ec->encode_matrix, k + p, k);

    ec_init_tables(k, p, &ec->encode_matrix[k * k], ec->g_tbls);

    return 0;
}


struct ec_runtime *
ec_get_runtime(void)
{
    return ec;
}

int
encode(struct ec_runtime *ec, char **data, char **parity, int len)
{
    ec_encode_data(len, ec->k, ec->p, ec->g_tbls, data, parity);
    return 0;
}

/*
 * ec_encode_data_update wrapper with temporal store.
 * e.g. pass delta data in to update parity
 * warning: non_temporal is pipelined by default
*/
inline int
encode_update(struct ec_runtime *ec, char *data, char **parity, int len, int vec_i, int non_temporal)
{   
    assert(non_temporal == PANGOLIN_EC_TEMPORAL);
    ec_encode_data_update(len, ec->k, ec->p, vec_i, ec->g_tbls, data, parity);
    return 0;
}

// temopral store with clwb
int
encode_update_pipeline(struct ec_runtime *ec, char *data, char **parity, int len, int vec_i, int non_temporal)
{
    assert(non_temporal == PANGOLIN_EC_TEMPORAL);
#ifdef PANGOLIN_AVX512CODE
    ec_encode_data_update_avx512_clwb(len, ec->k, ec->p, vec_i, ec->g_tbls, data, parity);
#else
    ec_encode_data_update_sse_clwb(len, ec->k, ec->p, vec_i, ec->g_tbls, data, parity);
#endif
    return 0;
}

/**
 * normal inplace delta update without clwb w/o mid buffer
*/
int
update_parity_inplace(struct ec_runtime *ec, char *data_i, char **parity, int len, int vec_i, 
                      char *update_content, int non_temporal)
{   
    assert(non_temporal == PANGOLIN_EC_TEMPORAL);

    if (ec->p == 1) {  // xor works
        void *array[4] = {data_i, update_content, parity[0], parity[0]};  // parity:=data+newd+parity
#ifdef PANGOLIN_AVX512CODE
        xor_gen_avx512_pgl(4, len, array);
#else
        xor_gen_sse_pgl(4, len, array);
#endif
    } else {
        // read data_i, gen delta data, gen delta parity and update parity
#ifdef PANGOLIN_AVX512CODE
        ec_encode_data_update_avx512_aio(len, ec->k, ec->p, vec_i, ec->g_tbls, data_i, parity, update_content);
#else
        ec_encode_data_update_sse_aio(len, ec->k, ec->p, vec_i, ec->g_tbls, data_i, parity, update_content);
#endif
    }
    return 0;
}

/**
 * inplace delta update with pipeline and clwb parity
 */ 
int
update_parity_inplace_pipeline(struct ec_runtime *ec, char *data_i, char **parity, int len, int vec_i, 
                      char *update_content, int non_temporal)
{
    assert(non_temporal == PANGOLIN_EC_TEMPORAL);
    if (ec->p == 1) {  // use simple and fast xor
        void *array[4] = {data_i, update_content, parity[0], parity[0]};  // parity:=data+newd+parity
        xor_gen_avx512_pipeline_pgl(4, len, array);
    } else {  // general case (only support 2 for now)
        // read data_i, gen delta data, gen delta parity and update parity
#ifdef PANGOLIN_AVX512CODE
        ec_encode_data_update_avx512_clwb_aio(len, ec->k, ec->p, vec_i, ec->g_tbls, data_i, parity, update_content);
#else
        ec_encode_data_update_sse_clwb_aio(len, ec->k, ec->p, vec_i, ec->g_tbls, data_i, parity, update_content);
#endif
    }
    return 0;
}

char *buffer = NULL;

/**
 * inplace delta update with mid buffer (except for xor parity)
*/
int
update_parity(struct ec_runtime *ec, char *data_i, char **parity, int len, int vec_i, 
                      char *update_content, int non_temporal)
{
    assert(non_temporal == PANGOLIN_EC_TEMPORAL);
    if (ec->p == 1) {  // xor works
        void *array[4] = {data_i, update_content, parity[0], parity[0]};  // parity:=data+newd+parity
        xor_gen_avx512_pgl(4, len, array);
    } else {  // rs code
        // ec_encode_data_update_avx512_aio(len, ec->k, ec->p, vec_i, ec->g_tbls, data_i, parity, update_content);
        assert(ec->p == 2);
        if (buffer == NULL) {
            buffer = (char*) malloc(2*(4096+256));
        }
        memset(buffer, 0, 2*len);
        char *buffer_0 = buffer;
        char *buffer_1 = buffer_0 + len;
        // new_data + old_data = delta_data
        {
            void *array[3] = {data_i, update_content, buffer_1};
            xor_gen_avx512_pgl(3, len, array);
        }
        // delta_data -> delta_parity
        void *buffer_p[2] = {buffer_0, buffer_1};
        ec_encode_data_update_avx512(len, ec->k, ec->p, vec_i, ec->g_tbls, buffer_1, buffer_p);
        // delta_parity + old_parity = new_parity
        for (int i = 0; i < ec->p; i++) {
            void *array[3] = {buffer_p[i], parity[i], parity[i]};
            xor_gen_avx512_pgl(3, len, array);
        } 
    }
    return 0;
}

/**
 * inplace delta update with pipeline and mid buffer (except for xor parity)
*/
int
update_parity_pipeline(struct ec_runtime *ec, char *data_i, char **parity, int len, int vec_i, 
                      char *update_content, int non_temporal)
{
    assert(non_temporal == PANGOLIN_EC_TEMPORAL);
    if (ec->p == 1) {  // xor works
        void *array[4] = {data_i, update_content, parity[0], parity[0]};  // parity:=data+newd+parity
        xor_gen_avx512_pipeline_pgl(4, len, array);
    } else {  // rs code
        // ec_encode_data_update_avx512_clwb_aio(len, ec->k, ec->p, vec_i, ec->g_tbls, data_i, parity, update_content);
        assert(ec->p == 2);
        if (buffer == NULL) {
            buffer = (char*) malloc(2*(4096+256));
        }
        memset(buffer, 0, 2*len);
        char *buffer_0 = buffer;
        char *buffer_1 = buffer_0 + len;
        // new_data + old_data = delta_data
        {
            void *array[3] = {data_i, update_content, buffer_1};
            xor_gen_avx512_pgl(3, len, array);
        }
        // delta_data -> delta_parity
        void *buffer_p[2] = {buffer_0, buffer_1};
        ec_encode_data_update_avx512(len, ec->k, ec->p, vec_i, ec->g_tbls, buffer_1, buffer_p);
        // delta_parity + old_parity = new_parity
        for (int i = 0; i < ec->p; i++) {
            void *array[3] = {buffer_p[i], parity[i], parity[i]};
            xor_gen_avx512_pipeline_pgl(3, len, array);
        } 
    }
    return 0;
}

/**
 * atomic update parity
*/
int
atomic_update_parity_inplace(struct ec_runtime *ec, char **parities, char **delta_parities, int len)
{
    size_t words = len >> 3;
    size_t flush_batches = words >> 3;  // 64B -> 1 flush
    int i = 0;
/* 1. words level */
    for (int p_i = 0; p_i < ec->p; p_i++) {
        for (i = 0; i < words; i++) {
            __atomic_xor_fetch(&(parities[p_i][i]), delta_parities[p_i][i], __ATOMIC_RELAXED);
        }
#ifdef PANGOLIN_PIPELINE
        // if (((i + 1) & 7) == 0) _mm_clflushopt(&(parities[p_i][i-7]));
#endif
    }
#ifdef PANGOLIN_PIPELINE
    // not 64B aligned -> extra flush
    for (int p_i = 0; p_i < ec->p; p_i++)
        // if (unlikely((i & 7) != 0)) _mm_clflushopt(&(parities[p_i][i - i % 8]));
#endif
        

/* 2. flush_batches level */
//     for (int p_i = 0; p_i < ec->p; p_i++) {
//         for (i = 0; i < flush_batches; i++) {
//             __atomic_xor_fetch(&parities[p_i][i*8  ], delta_parities[p_i][i*8  ], __ATOMIC_RELAXED);
//             __atomic_xor_fetch(&parities[p_i][i*8+1], delta_parities[p_i][i*8+1], __ATOMIC_RELAXED);
//             __atomic_xor_fetch(&parities[p_i][i*8+2], delta_parities[p_i][i*8+2], __ATOMIC_RELAXED);
//             __atomic_xor_fetch(&parities[p_i][i*8+3], delta_parities[p_i][i*8+3], __ATOMIC_RELAXED);
//             __atomic_xor_fetch(&parities[p_i][i*8+4], delta_parities[p_i][i*8+4], __ATOMIC_RELAXED);
//             __atomic_xor_fetch(&parities[p_i][i*8+5], delta_parities[p_i][i*8+5], __ATOMIC_RELAXED);
//             __atomic_xor_fetch(&parities[p_i][i*8+6], delta_parities[p_i][i*8+6], __ATOMIC_RELAXED);
//             __atomic_xor_fetch(&parities[p_i][i*8+7], delta_parities[p_i][i*8+7], __ATOMIC_RELAXED);
//             _mm_clflushopt(&parities[p_i][i*8]);
//         }
//     }

//     // not 64B aligned -> extra write and flush
//     for (int p_i = 0; p_i < ec->p; p_i++) {
//         if (flush_batches * 8 != words) {
//             for (i = flush_batches*8; i < words; i++) {
//                 __atomic_xor_fetch(&parities[p_i][i], delta_parities[p_i][i], __ATOMIC_RELAXED);
//             }
// #ifdef PANGOLIN_PIPELINE
//             _mm_clflushopt(&parities[p_i][flush_batches*8]);
// #endif
//         }
//     }
    return 0;
}

/**
 * gen_delta_parity for atomic update
*/
int
gen_delta_parity(struct ec_runtime *ec, char *data_i, int len, int vec_i, 
                      char *update_content, char** delta_parity)
{   
    // char delta_data[4096];
    if (update_content != NULL) {
        // data_i ^ update_content = delta data => delta_parity[-1]
        void *array[3] = {data_i, update_content, delta_parity[1]};
        // void *array[3] = {data_i, update_content, delta_data};
// #ifdef PANGOLIN_AVX512CODE
        xor_gen_avx512_pgl(3, len, array);
        // delta_data -> delta_parity (use delta_parity[1] instead data_i)
        ec_encode_data_update_avx512(len, ec->k, ec->p, vec_i, ec->g_tbls, delta_parity[1], delta_parity);
// #else
//         xor_gen_sse_pgl(3, len, array);
//         ec_encode_data_update_sse(len, ec->k, ec->p, vec_i, ec->g_tbls, delta_parity[1], delta_parity);
// #endif
    } else {
        // delta_data -> delta_parity (data_i is delta data)
// #ifdef PANGOLIN_AVX512CODE
        ec_encode_data_update_avx512(len, ec->k, ec->p, vec_i, ec->g_tbls, data_i, delta_parity);
// #else
//         ec_encode_data_update_sse(len, ec->k, ec->p, vec_i, ec->g_tbls, data_i, delta_parity);
// #endif
    }
    return 0;
}
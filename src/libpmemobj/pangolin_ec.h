/**
 * a simple C lang version of ec.hh in local-pm-ec to provide SIMD coding features
 */

#ifndef PANGOLIN_EC_H
#define PANGOLIN_EC_H 1

#define PANGOLIN_EC_NON_TEMPORAL 1
#define PANGOLIN_EC_TEMPORAL 0

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include <isa-l.h>

struct ec_runtime {
    int k;
    int p;
    // int len;
    // int num_thread;

    // for rs code
    char *g_tbls;
    char *encode_matrix;
};

int
init_ec_runtime(int k, int p, int len);


struct ec_runtime *
ec_get_runtime(void);

int
encode(struct ec_runtime *ec, char **data, char **parity, int len);

/*
 * ec_encode_data_update wrapper with temporal store.
 * e.g. pass delta data in to update parity
 * warning: non_temporal is pipelined by default
*/
int
encode_update(struct ec_runtime *ec, char *data, char **parity, int len, int vec_i, int non_temporal);

// temopral store with clwb
int
encode_update_pipeline(struct ec_runtime *ec, char *data, char **parity, int len, int vec_i, int non_temporal);

// normal inplace delta update without clwb
int
update_parity_inplace(struct ec_runtime *ec, char *data_i, char **parity, int len, int vec_i, 
                      char *update_content, int non_temporal);

int
atomic_update_parity_inplace(struct ec_runtime *ec, char **parity, char **delta_parity, int len);

/**
 * inplace delta update with pipeline and clwb parity
 */ 
int
update_parity_inplace_pipeline(struct ec_runtime *ec, char *data_i, char **parity, int len, int vec_i, 
                      char *update_content, int non_temporal);

/**
 * gen_delta_parity for atomic update
*/
int
gen_delta_parity(struct ec_runtime *ec, char *data_i, int len, int vec_i, 
                      char *update_content, char** delta_parity);

#endif
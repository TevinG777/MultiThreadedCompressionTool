// Group 31 Operating Systems Project 2

// Tevin Gajadhar #U89310811
// Ethan Varn U41240412
// Danish Abdullah U25632651
// Jack Rodriguez U69523108

// Multithreaded text compressor for Project 2.
// Uses pthreads and zlib so multiple worker threads can compress .txt files in parallel and write a single ordered output file.

#ifndef __SERIAL_H__
#define __SERIAL_H__

#include <stdio.h>
#include <pthread.h>
#include <zlib.h>
#define MAX_WORKER_THREADS 19

typedef struct compression_context {
    // inputs
    const char *directory_name;  
    char **files;
    int file_count;                   

    // work distribution
    int next_index;               // next file index to claim
    pthread_mutex_t index_lock;   // protects next_index

    // ordered output
    int next_write_index;         // next index allowed to write
    pthread_mutex_t write_lock;   // protects writing + counters
    pthread_cond_t  write_cond;   // workers wait until it's their turn

    // output sink
    FILE *f_out;

    int total_in;
    int total_out;
} compression_context_t;

typedef struct {
    compression_context_t *ctx;  // shared context (files list, locks, etc.)
    z_stream stream;             // zlib state for this thread
    unsigned char *buffer_in;    // input buffer (read chunks from file)
    unsigned char *buffer_out;   // output buffer (compressed data)
} worker_state_t;


int compress_directory(char *directory_name);

#ifdef SERIAL_IMPLEMENTATION
int list_txt_files(const char *dir, char ***out_files, int *out_n);
void init_context(compression_context_t *ctx, const char *dir, char **files, int n, FILE *f_out);
void destroy_context(compression_context_t *ctx);
void *compression_worker(void *arg);
int spawn_workers(compression_context_t *ctx, int worker_count);
#endif

#endif

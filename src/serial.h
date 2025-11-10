#ifndef __SERIAL_H__
#define __SERIAL_H__

#include <stdio.h>

typedef struct compression_context {
	const char *directory;
	char **files;
	int file_count;
	int next_index;
	FILE *f_out;
} compression_context_t;

int compress_directory(char *directory_name);

#ifdef SERIAL_IMPLEMENTATION
int list_txt_files(const char *dir, char ***out_files, int *out_n);
void init_context(compression_context_t *ctx, const char *dir, char **files, int n, FILE *f_out);
void destroy_context(compression_context_t *ctx);
void *compression_worker(void *arg);
int spawn_workers(compression_context_t *ctx, int worker_count);
#endif

#endif

// Tevin Gajadhar #U89310811

#define SERIAL_IMPLEMENTATION

#include "serial.h"
#include <pthread.h>
#include <assert.h>
#include <stdio.h>
#include <dirent.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define BUFFER_SIZE 1048576 // 1MB

static int cmp(const void *a, const void *b) {
	return strcmp(*(char *const *) a, *(char *const *) b);
}

int list_txt_files(const char *dir, char ***out_files, int *out_n) {
	(void)dir;

	if(out_files != NULL)
		*out_files = NULL;
	if(out_n != NULL)
		*out_n = 0;

	fprintf(stderr, "list_txt_files is a placeholder - implement me!\n");
	return 0;
}

void init_context(compression_context_t *ctx, const char *dir, char **files, int n, FILE *f_out) {
	assert(ctx != NULL);
	ctx->directory = dir;
	ctx->files = files;
	ctx->file_count = n;
	ctx->next_index = 0;
	ctx->f_out = f_out;
}

void destroy_context(compression_context_t *ctx) {
	if(ctx == NULL)
		return;

	if(ctx->files != NULL) {
		for(int i = 0; i < ctx->file_count; i++)
			free(ctx->files[i]);
		free(ctx->files);
	}

	ctx->files = NULL;
	ctx->file_count = 0;
	ctx->next_index = 0;
	ctx->f_out = NULL;
}

// Tevin Gajadhar #U89310811
void *compression_worker(void *arg) {
	(void)arg;
	fprintf(stderr, "compression_worker is a placeholder – implement me!\n");
	return NULL;
}

//Jack Rodriguez U69523108
int spawn_workers(compression_context_t *ctx, int worker_count) 
{
	//If there are no files do nothing
	if (!ctx || ctx->file_count <= 0)
	{
		return 0;
	} 

	//if there is no current workers
    if (worker_count <= 0)
	{
		//set the number of workers to the number of files.
		worker_count = ctx->file_count;
	} 
	//if there are more workers than files
    if (worker_count > ctx->file_count)
	{
		//set worker count to the file count
		worker_count = ctx->file_count;
	} 
	//if there are more workers than the max allowed, set it to max
    if (worker_count > MAX_WORKER_THREADS)
	{
		worker_count = MAX_WORKER_THREADS;
	} 

	//makes 19 threads
    pthread_t th[MAX_WORKER_THREADS];

	//------failed part way through ---------
	//loop runs once for each worker count
    for (int i = 0; i < worker_count; i++) 
	{
		//if there is a thread
        if (pthread_create(&th[i], NULL, compression_worker, ctx) != 0) 
		{
			//decrements i and run pthread for each thread so that they join back
            while (i--)
			{
				pthread_join(th[i], NULL);
			} 
			//if it returns here we have an error
            return -1;
        }
    }
//------successful--------
	//for each worker
    for (int i = 0; i < worker_count; i++)
	{
		//joins
		pthread_join(th[i], NULL);
	}


	//function done
    return 0;
}

int compress_directory(char *directory_name) {
	if(directory_name == NULL)
		return -1;

	compression_context_t ctx;
	memset(&ctx, 0, sizeof(ctx));

	if(list_txt_files(directory_name, &ctx.files, &ctx.file_count) != 0) {
		fprintf(stderr, "list_txt_files failed.\n");
		return -1;
	}

	FILE *f_out = fopen("text.tzip", "wb");
	if(f_out == NULL) {
		perror("text.tzip");
		destroy_context(&ctx);
		return -1;
	}

	init_context(&ctx, directory_name, ctx.files, ctx.file_count, f_out);

	int worker_status = spawn_workers(&ctx, 0);

	fclose(f_out);
	destroy_context(&ctx);

	if(worker_status != 0) {
		fprintf(stderr, "spawn_workers failed – the template is unfinished.\n");
		return worker_status;
	}

	printf("Compression rate: 0.00%%\n");
	return 0;
}

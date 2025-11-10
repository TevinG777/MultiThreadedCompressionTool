#include <dirent.h>
#include <pthread.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>

#define BUFFER_SIZE 1048576 // 1MB
#define MAX_WORKER_THREADS 19

int cmp(const void *a, const void *b) {
	return strcmp(*(char **) a, *(char **) b);
}

typedef struct {
	const char *directory_name;
	char **files;
	int nfiles;
	int next_index;
	pthread_mutex_t index_lock;
	unsigned char **compressed_data;
	int *compressed_sizes;
	int *original_sizes;
} compression_context_t;

static void *compression_worker(void *arg) {
	compression_context_t *ctx = (compression_context_t *) arg;

	while(1) {
		pthread_mutex_lock(&ctx->index_lock);
		int idx = ctx->next_index++;
		pthread_mutex_unlock(&ctx->index_lock);

		if(idx >= ctx->nfiles)
			break;

		int len = strlen(ctx->directory_name)+strlen(ctx->files[idx])+2;
		char *full_path = malloc(len*sizeof(char));
		assert(full_path != NULL);
		strcpy(full_path, ctx->directory_name);
		strcat(full_path, "/");
		strcat(full_path, ctx->files[idx]);

		unsigned char buffer_in[BUFFER_SIZE];
		unsigned char buffer_out[BUFFER_SIZE];

		FILE *f_in = fopen(full_path, "rb");
		assert(f_in != NULL);
		int nbytes = fread(buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);
		ctx->original_sizes[idx] = nbytes;

		z_stream strm;
		memset(&strm, 0, sizeof(z_stream));
		int ret = deflateInit(&strm, 9);
		assert(ret == Z_OK);
		strm.avail_in = nbytes;
		strm.next_in = buffer_in;
		strm.avail_out = BUFFER_SIZE;
		strm.next_out = buffer_out;

		ret = deflate(&strm, Z_FINISH);
		assert(ret == Z_STREAM_END);

		int nbytes_zipped = BUFFER_SIZE-strm.avail_out;
		ctx->compressed_data[idx] = malloc(nbytes_zipped*sizeof(unsigned char));
		assert(ctx->compressed_data[idx] != NULL);
		memcpy(ctx->compressed_data[idx], buffer_out, nbytes_zipped*sizeof(unsigned char));
		ctx->compressed_sizes[idx] = nbytes_zipped;

		ret = deflateEnd(&strm);
		assert(ret == Z_OK);

		free(full_path);
	}

	return NULL;
}

int compress_directory(char *directory_name) {
	DIR *d;
	struct dirent *dir;
	char **files = NULL;
	int nfiles = 0;

	d = opendir(directory_name);
	if(d == NULL) {
		printf("An error has occurred\n");
		return 0;
	}

	// create sorted list of text files
	while ((dir = readdir(d)) != NULL) {
		int len = strlen(dir->d_name);
		if(len >= 4 && dir->d_name[len-4] == '.' && dir->d_name[len-3] == 't' && dir->d_name[len-2] == 'x' && dir->d_name[len-1] == 't') {
			files = realloc(files, (nfiles+1)*sizeof(char *));
			assert(files != NULL);

			files[nfiles] = strdup(dir->d_name);
			assert(files[nfiles] != NULL);

			nfiles++;
		}
	}
	closedir(d);
	qsort(files, nfiles, sizeof(char *), cmp);

	unsigned char **compressed_data = NULL;
	int *compressed_sizes = NULL;
	int *original_sizes = NULL;
	if(nfiles > 0) {
		compressed_data = calloc(nfiles, sizeof(unsigned char *));
		compressed_sizes = calloc(nfiles, sizeof(int));
		original_sizes = calloc(nfiles, sizeof(int));
		assert(compressed_data != NULL && compressed_sizes != NULL && original_sizes != NULL);

		compression_context_t ctx;
		ctx.directory_name = directory_name;
		ctx.files = files;
		ctx.nfiles = nfiles;
		ctx.next_index = 0;
		ctx.compressed_data = compressed_data;
		ctx.compressed_sizes = compressed_sizes;
		ctx.original_sizes = original_sizes;
		pthread_mutex_init(&ctx.index_lock, NULL);

		int worker_count = nfiles < MAX_WORKER_THREADS ? nfiles : MAX_WORKER_THREADS;
		if(worker_count == 0)
			worker_count = 1;

		pthread_t threads[MAX_WORKER_THREADS];
		for(int i=0; i<worker_count; i++) {
			int ret = pthread_create(&threads[i], NULL, compression_worker, &ctx);
			assert(ret == 0);
		}
		for(int i=0; i<worker_count; i++) {
			int ret = pthread_join(threads[i], NULL);
			assert(ret == 0);
		}
		pthread_mutex_destroy(&ctx.index_lock);
	}

	// create a single zipped package with all text files in lexicographical order
	int total_in = 0, total_out = 0;
	FILE *f_out = fopen("text.tzip", "wb");
	assert(f_out != NULL);
	for(int i=0; i < nfiles; i++) {
		fwrite(&compressed_sizes[i], sizeof(int), 1, f_out);
		fwrite(compressed_data[i], sizeof(unsigned char), compressed_sizes[i], f_out);
		total_in += original_sizes[i];
		total_out += compressed_sizes[i];
		free(compressed_data[i]);
	}
	fclose(f_out);

	if(nfiles == 0 || total_in == 0)
		printf("Compression rate: 0.00%%\n");
	else
		printf("Compression rate: %.2lf%%\n", 100.0*(total_in-total_out)/total_in);

	// release list of files
	for(int i=0; i < nfiles; i++)
		free(files[i]);
	free(files);
	free(compressed_data);
	free(compressed_sizes);
	free(original_sizes);

	// do not modify the main function after this point!
	return 0;
}

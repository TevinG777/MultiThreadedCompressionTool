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
	int next_write_index;
	pthread_mutex_t index_lock;
	pthread_mutex_t write_lock;
	pthread_cond_t write_cond;
	FILE *f_out;
	long long total_in;
	long long total_out;
} compression_context_t;

typedef struct {
	compression_context_t *ctx;
	z_stream stream;
	unsigned char *buffer_in;
	unsigned char *buffer_out;
} worker_state_t;

static void *compression_worker(void *arg) {
	worker_state_t *worker = (worker_state_t *) arg;
	compression_context_t *ctx = worker->ctx;
	z_stream *strm = &worker->stream;

	memset(strm, 0, sizeof(z_stream));
	int ret = deflateInit(strm, Z_BEST_COMPRESSION);
	assert(ret == Z_OK);

	while(1) {
		pthread_mutex_lock(&ctx->index_lock);
		int idx = ctx->next_index++;
		pthread_mutex_unlock(&ctx->index_lock);

		if(idx >= ctx->nfiles)
			break;

		size_t len = strlen(ctx->directory_name) + strlen(ctx->files[idx]) + 2;
		char *full_path = malloc((len+1) * sizeof(char));
		assert(full_path != NULL);
		strcpy(full_path, ctx->directory_name);
		strcat(full_path, "/");
		strcat(full_path, ctx->files[idx]);

		FILE *f_in = fopen(full_path, "rb");
		assert(f_in != NULL);
			int nbytes = fread(worker->buffer_in, sizeof(unsigned char), BUFFER_SIZE, f_in);
		fclose(f_in);
		free(full_path);

		ret = deflateReset(strm);
		assert(ret == Z_OK);
		strm->avail_in = nbytes;
		strm->next_in = worker->buffer_in;
		strm->avail_out = BUFFER_SIZE;
		strm->next_out = worker->buffer_out;

		ret = deflate(strm, Z_FINISH);
		assert(ret == Z_STREAM_END);

		int nbytes_zipped = BUFFER_SIZE - strm->avail_out;

		pthread_mutex_lock(&ctx->write_lock);
		while(idx != ctx->next_write_index)
			pthread_cond_wait(&ctx->write_cond, &ctx->write_lock);

		fwrite(&nbytes_zipped, sizeof(int), 1, ctx->f_out);
		fwrite(worker->buffer_out, sizeof(unsigned char), nbytes_zipped, ctx->f_out);
		ctx->total_in += nbytes;
		ctx->total_out += nbytes_zipped;
		ctx->next_write_index++;
		pthread_cond_broadcast(&ctx->write_cond);
		pthread_mutex_unlock(&ctx->write_lock);
	}

	deflateEnd(strm);
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

	FILE *f_out = fopen("text.tzip", "wb");
	assert(f_out != NULL);
	setvbuf(f_out, NULL, _IOFBF, BUFFER_SIZE);

	compression_context_t ctx;
	ctx.directory_name = directory_name;
	ctx.files = files;
	ctx.nfiles = nfiles;
	ctx.next_index = 0;
	ctx.next_write_index = 0;
	ctx.f_out = f_out;
	ctx.total_in = 0;
	ctx.total_out = 0;
	pthread_mutex_init(&ctx.index_lock, NULL);
	pthread_mutex_init(&ctx.write_lock, NULL);
	pthread_cond_init(&ctx.write_cond, NULL);

	if(nfiles > 0) {
			int worker_count = nfiles < MAX_WORKER_THREADS ? nfiles : MAX_WORKER_THREADS;
			if(worker_count == 0)
				worker_count = 1;

			pthread_t threads[MAX_WORKER_THREADS];
			worker_state_t *worker_states = calloc(worker_count, sizeof(worker_state_t));
			assert(worker_states != NULL);

			for(int i=0; i<worker_count; i++) {
				worker_states[i].ctx = &ctx;
				worker_states[i].buffer_in = malloc(BUFFER_SIZE);
				worker_states[i].buffer_out = malloc(BUFFER_SIZE);
				assert(worker_states[i].buffer_in != NULL && worker_states[i].buffer_out != NULL);
				int ret = pthread_create(&threads[i], NULL, compression_worker, &worker_states[i]);
				assert(ret == 0);
			}
			for(int i=0; i<worker_count; i++) {
				int ret = pthread_join(threads[i], NULL);
				assert(ret == 0);
				free(worker_states[i].buffer_in);
				free(worker_states[i].buffer_out);
			}
			free(worker_states);
		}

	pthread_mutex_destroy(&ctx.index_lock);
	pthread_mutex_destroy(&ctx.write_lock);
	pthread_cond_destroy(&ctx.write_cond);
	fclose(f_out);

	if(ctx.total_in == 0)
		printf("Compression rate: 0.00%%\n");
	else
		printf("Compression rate: %.2lf%%\n", 100.0*(ctx.total_in-ctx.total_out)/ctx.total_in);

	// release list of files
	for(int i=0; i < nfiles; i++)
		free(files[i]);
	free(files);
	// do not modify the main function after this point!
	return 0;
}

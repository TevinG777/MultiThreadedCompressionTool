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

// Ethan Varn U41240412
int list_txt_files(const char *dir, char ***out_files, int *out_n) {
	// ensure arguments are properly initialized
	if(out_files != NULL)
		*out_files = NULL;
	if(out_n != NULL)
		*out_n = 0;
	
	// open directory to read file names
	DIR* d = opendir(dir);
	if (d == NULL) {
		fprintf(stderr, "Invalid directory.\n");
		return -1;
	}
		
	// local variables to construct file name array
	char **files = NULL;
	int n_files = 0;
	int capacity = 0; // total space of the allocated memory at *files (reduces realloc() calls)
	
	struct dirent* entry;
	while((entry = readdir(d)) != NULL) {
		const char* name = entry->d_name;

		// skip hidden files ".", ".."
		if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0) {
			continue;
		}

		// ensure files include .txt extension
		int name_len = strlen(entry->d_name);
		if (name_len >= 4 && name[name_len-4] == '.' && name[name_len-3] == 't' && name[name_len-2] == 'x' && name[name_len-1] == 't') {
			// file name includes .txt extension, expand array if necessary and copy name
			if (n_files == capacity) { // need to allocate more room
				capacity = (capacity == 0) ? 16 : capacity * 2;
				char **temp = realloc(files, capacity * sizeof(char*));
				assert(temp != NULL);
				files = temp;
			}
			// append filename to files array
			files[n_files] = strdup(name);
			assert(files[n_files] != NULL);
			++n_files;
		}
	}
	
	// sort out_files alphabetically
	if (n_files > 1) {
		qsort(files, n_files, sizeof(char*), cmp);
	}

	closedir(d);

	// prepare outputs
	if(out_files != NULL)
		*out_files = files;
	else {
		for(int i = 0; i < n_files; ++i)
			free(files[i]);
		free(files);
		files = NULL;
	}

	if(out_n != NULL)
		*out_n = n_files;
		
	return 0;
}

void init_context(compression_context_t *ctx, const char *dir, char **files, int n, FILE *f_out) {
	assert(ctx != NULL);
	ctx->directory_name = dir;
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

	// cast arg to worker_state_t pointer to access context so that we can access the global context
	worker_state_t *w = (worker_state_t *)arg;
    compression_context_t *ctx = w->ctx;

	// initialize zlib stream and set up for compression
	z_stream *s = &w->stream;
    memset(s, 0, sizeof(*s));
    if (deflateInit(s, Z_BEST_COMPRESSION) != Z_OK) return NULL;

	// allocate input and output buffers
	if (!w->buffer_in)  w->buffer_in  = (unsigned char*)malloc(BUFFER_SIZE);
    if (!w->buffer_out) w->buffer_out = (unsigned char*)malloc(BUFFER_SIZE);

	// If either buffer allocation failed, clean up and exit
    if (!w->buffer_in || !w->buffer_out) { deflateEnd(s); return NULL; }

	// Variable to detect if we need to abort a file
	int abort_file = 0;

	while (1) {
		// Claim the next file to process and increment the index 
		pthread_mutex_lock(&ctx->index_lock);

		// Get the next file index to process and dtermine if there are more files to process
		int i = ctx->next_index++;
		int done = (i >= ctx->file_count);
		pthread_mutex_unlock(&ctx->index_lock);

		// Break if all files have been processed
		if (done) break;

		// reset abort_file flag for the new file
		abort_file = 0;
        char path[4096];

		// Create the full file path using the directory name and file name
        snprintf(path, sizeof(path), "%s/%s", ctx->directory_name ? ctx->directory_name : ".", ctx->files[i]);

		// Open the file for reading in binary mode, if fail continue to next file
        FILE *fin = fopen(path, "rb");
        if (!fin) continue;

		// Create a tmp file to store the compressed data if fail close input file and continue
        FILE *ftmp = tmpfile();                
        if (!ftmp) { fclose(fin); continue; }

		// Reset zlib stream for new compression and intialize byte counters
        deflateReset(s);
        unsigned long long in_bytes = 0, out_bytes = 0;

		// Loop to read from input file, compress it and write to the temp file
		while(1) {
			// Read a chunk of data from the input file of size BUFFER_SIZE
            size_t rd = fread(w->buffer_in, 1, BUFFER_SIZE, fin);

			// If read error occurs, break the loop
            if (rd == 0 && ferror(fin)) break;

			// Update input byte counter and set zlib input buffer
            in_bytes += rd;
            s->next_in = w->buffer_in;
            s->avail_in = (uInt)rd;

			// Compress the data and write to the temp file
            int flush = feof(fin) ? Z_FINISH : Z_NO_FLUSH;

			int err = 0;

			// Loop to deflate until all input is consumed and output buffer is flushed
            do {

				// Set zlib output buffer
			    s->next_out  = w->buffer_out;	
			    s->avail_out = (uInt)BUFFER_SIZE;              

				// Perform deflation
			    int ret = deflate(s, flush);
			    if (ret == Z_STREAM_ERROR) {  
					err = 1;                
			        break;
			    }
				
				// Calculate number of bytes produced and write to temp file
			    size_t have = BUFFER_SIZE - s->avail_out;

				// If there are bytes to write then write them to the temp file
			    if (have) {
					// Write compressed data to the temporary file
			        size_t written = fwrite(w->buffer_out, 1, have, ftmp);
			        if (written != have) {  
						err = 1;                   
			            break;
			        }
			        out_bytes += written;
			    }
			} while (s->avail_in > 0 || (flush == Z_FINISH && s->avail_out == 0));

            if (err) { abort_file = 1; break; }
    		if (feof(fin)) break;        
        }

		// Close input, if abort_file is set continue to next file
		fclose(fin);
		if (abort_file) continue; 
		

		// Grab the write lock to write 
		pthread_mutex_lock(&ctx->write_lock);

		// Wait until it’s this worker’s turn to write
		while (i != ctx->next_write_index)
		    pthread_cond_wait(&ctx->write_cond, &ctx->write_lock);

		// rewind temp file and write its contents to the output file
		rewind(ftmp);

		// write temp file contents to output file
		for (;;) {
		    size_t rd2 = fread(w->buffer_out, 1, BUFFER_SIZE, ftmp);
		    if (!rd2) break;
		    fwrite(w->buffer_out, 1, rd2, ctx->f_out);
		}

		// flush output file to ensure data is written
		fflush(ctx->f_out);

		// update stats while holding the lock
		ctx->total_in  += (long long)in_bytes;
		ctx->total_out += (long long)out_bytes;

		// let next index write
		ctx->next_write_index++;
		pthread_cond_broadcast(&ctx->write_cond);
		pthread_mutex_unlock(&ctx->write_lock);

		// now it’s safe to close the temp file
		fclose(ftmp);

	}

	// Clean up zlib stream and buffers
	deflateEnd(s);
	free(w->buffer_in);
	free(w->buffer_out);
	w->buffer_in = w->buffer_out = NULL;
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
	// create sorted list of text files
	char **files = NULL;
	int nfiles = 0;

	if (list_txt_files(directory_name, &files, &nfiles) != 0) {
		fprintf(stderr, "Error creating .txt file names array.\n");
		return -1;
	}


	FILE *f_out = fopen("text.tzip", "wb");
	assert(f_out != NULL);
	setvbuf(f_out, NULL, _IOFBF, BUFFER_SIZE);

	compression_context_t ctx;
	ctx.directory_name = directory_name;
	ctx.files = files;
	ctx.file_count = nfiles;
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

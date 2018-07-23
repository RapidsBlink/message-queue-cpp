#pragma once

#include <cstdio>
#include <cstdint>
#include <atomic>
#include <string>
#include <thread>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <malloc.h>
#include <google/malloc_extension.h>


#include "blockingQueue.hpp"

#include "../../include/queue_store_config.h"
#include "../utils/log.h"
#include "Barrier.hpp"
#include "../utils/utils.h"

#define CLUSTER_SIZE (77)
#define INDEX_ENTRY_SIZE (42)
#define RAW_NORMAL_MESSAGE_SIZE (58)
#define MAX_CONCURRENT_INDEX_MAPPED_BLOCK_NUM 2
#define FILESYSTEM_BLOCK_SIZE 512
#define QUEUE_NUM_PER_IO_THREAD (TOTAL_QUEUE_NUM/IO_THREAD)
#define INDEX_MAPPED_BLOCK_RAW_SIZE (INDEX_ENTRY_SIZE * QUEUE_NUM_PER_IO_THREAD * CLUSTER_SIZE)
#define INDEX_MAPPED_BLOCK_PADDING_SIZE (FILESYSTEM_BLOCK_SIZE - INDEX_MAPPED_BLOCK_RAW_SIZE % FILESYSTEM_BLOCK_SIZE)
#define INDEX_MAPPED_BLOCK_ALIGNED_SIZE (INDEX_MAPPED_BLOCK_RAW_SIZE + INDEX_MAPPED_BLOCK_PADDING_SIZE)
#define INDEX_BLOCK_WRITE_TIMES_TO_FULL (INDEX_MAPPED_BLOCK_RAW_SIZE/INDEX_ENTRY_SIZE)
#define MAX_MAPED_CHUNK_NUM  (120l * 1024 * 1024 * 1024 / INDEX_MAPPED_BLOCK_RAW_SIZE / IO_THREAD)
#define LARGE_MESSAGE_MAGIC_CHAR (0)
#define DATA_FILE_MAX_SIZE (120l * 1024 * 1024 * 1024 / IO_THREAD)

using std::atomic;
using std::string;
using std::thread;

enum asyncfileio_thread_status {
    AT_CLOSING = 1, AT_RUNNING = 2, AT_INITING = 3
};


class asyncio_task_t {
public:
    ssize_t global_offset;

    asyncio_task_t() {
        global_offset = 0;
    }

    asyncio_task_t(ssize_t global_offset) {
        this->global_offset = global_offset;
    }
};

class asyncfileio_thread_t {
public:
    const int thread_id;

    atomic<long> data_file_current_size;
    int data_file_fd;
    size_t index_file_size;

    int index_file_fd;

    size_t current_index_mapped_start_offset;
    size_t current_index_mapped_end_offset;
    size_t current_index_mapped_start_chunk;
    size_t index_mapped_flush_start_chunkID;
    size_t index_mapped_flush_end_chunkID;
    atomic<int> *index_mapped_block_write_counter;
    std::mutex *mapped_block_mtx;
    std::condition_variable *mapped_block_cond;

    char **index_file_memory_block;

    uint32_t *queue_counter;

    BlockingQueue<asyncio_task_t *> *blockingQueue;
    enum asyncfileio_thread_status status;

    asyncfileio_thread_t(int tid, std::string file_prefix) : thread_id(tid) {

        this->data_file_current_size.store(0);
        this->index_file_size = 0;

        index_mapped_block_write_counter = new atomic<int>[MAX_MAPED_CHUNK_NUM];
        index_file_memory_block = new char *[MAX_MAPED_CHUNK_NUM];
        for (int i = 0; i < MAX_MAPED_CHUNK_NUM; i++) {
            index_mapped_block_write_counter[i].store(0);
            index_file_memory_block[i] = nullptr;
        }

        for (int i = 0; i < MAX_CONCURRENT_INDEX_MAPPED_BLOCK_NUM; i++) {
            index_file_memory_block[i] = (char *) memalign(FILESYSTEM_BLOCK_SIZE, INDEX_MAPPED_BLOCK_ALIGNED_SIZE);
//            index_file_memory_block[i] = ;new char[INDEX_MAPPED_BLOCK_SIZE];
        }

        queue_counter = new uint32_t[QUEUE_NUM_PER_IO_THREAD];
        memset(queue_counter, 0, sizeof(uint32_t) * QUEUE_NUM_PER_IO_THREAD);
        mapped_block_mtx = new std::mutex[MAX_MAPED_CHUNK_NUM];
        mapped_block_cond = new std::condition_variable[MAX_MAPED_CHUNK_NUM];

        this->blockingQueue = new BlockingQueue<asyncio_task_t *>;

        string tmp_str = file_prefix + "_" + std::to_string(tid) + ".data";
        this->data_file_fd = open(tmp_str.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        ftruncate(data_file_fd, 0);
        ftruncate(data_file_fd, DATA_FILE_MAX_SIZE);

        tmp_str = file_prefix + "_" + std::to_string(tid) + ".idx";
        this->index_file_fd = open(tmp_str.c_str(), O_WRONLY | O_CREAT | O_DIRECT | O_SYNC, S_IRUSR | S_IWUSR);
        ftruncate(index_file_fd, 0);

        this->current_index_mapped_start_chunk = 0;
        this->current_index_mapped_start_offset = 0;
        this->current_index_mapped_end_offset =
                ((size_t) INDEX_MAPPED_BLOCK_ALIGNED_SIZE) * MAX_CONCURRENT_INDEX_MAPPED_BLOCK_NUM;
        this->index_file_size = 0;
    }

    void doIO(asyncio_task_t *asyncio_task) {
        for (; current_index_mapped_start_chunk < MAX_MAPED_CHUNK_NUM; current_index_mapped_start_chunk++) {
            if (index_mapped_block_write_counter[current_index_mapped_start_chunk].load() >=
                INDEX_BLOCK_WRITE_TIMES_TO_FULL) {

                index_file_size += INDEX_MAPPED_BLOCK_ALIGNED_SIZE;
                ftruncate(index_file_fd, index_file_size);

                pwrite(index_file_fd, index_file_memory_block[current_index_mapped_start_chunk],
                       INDEX_MAPPED_BLOCK_ALIGNED_SIZE,
                       INDEX_MAPPED_BLOCK_ALIGNED_SIZE * current_index_mapped_start_chunk);

                int next_chunk = current_index_mapped_start_chunk + MAX_CONCURRENT_INDEX_MAPPED_BLOCK_NUM;
                current_index_mapped_start_offset += INDEX_MAPPED_BLOCK_ALIGNED_SIZE;
                current_index_mapped_end_offset += INDEX_MAPPED_BLOCK_ALIGNED_SIZE;

                {
                    std::unique_lock<std::mutex> lock(mapped_block_mtx[next_chunk]);
                    index_file_memory_block[next_chunk] = index_file_memory_block[current_index_mapped_start_chunk];
                }
                mapped_block_cond[next_chunk].notify_all();

                index_file_memory_block[current_index_mapped_start_chunk] = nullptr;

                log_info("io thread %d advanced to %d", this->thread_id, next_chunk);

            } else {
                break;
            }
        }
    }
};

bool allFlushFlag = false;
bool ioFinished = false;
Barrier *barrier;
atomic<int> *finish_thread_counter;

void ioThreadFunction(asyncfileio_thread_t *args) {
    asyncfileio_thread_t *work_thread = args;
    work_thread->status = AT_RUNNING;

    for (;;) {
        asyncio_task_t *task = work_thread->blockingQueue->take();

        if (work_thread->status == AT_CLOSING || task->global_offset == -1) {
            size_t force_flush_chunk_num = 0;
            if (work_thread->thread_id < 1) {
                force_flush_chunk_num = 1;
            }
            work_thread->index_mapped_flush_start_chunkID =
                    work_thread->current_index_mapped_start_chunk + force_flush_chunk_num;
            work_thread->index_mapped_flush_end_chunkID = work_thread->index_mapped_flush_start_chunkID;
            for (size_t i = work_thread->current_index_mapped_start_chunk;
                 i < work_thread->index_mapped_flush_start_chunkID &&
                 work_thread->index_file_memory_block[i] != nullptr; i++) {
                work_thread->index_file_size += INDEX_MAPPED_BLOCK_ALIGNED_SIZE;
                ftruncate(work_thread->index_file_fd, work_thread->index_file_size);

                pwrite(work_thread->index_file_fd, work_thread->index_file_memory_block[i],
                       INDEX_MAPPED_BLOCK_ALIGNED_SIZE, INDEX_MAPPED_BLOCK_ALIGNED_SIZE * i);
                free(work_thread->index_file_memory_block[i]);
            }
            fsync(work_thread->index_file_fd);
            finish_thread_counter->fetch_add(1);
            ftruncate(work_thread->data_file_fd, work_thread->data_file_current_size.load());
            break;
        }
        work_thread->doIO(task);
        delete task;
    }
}


void flush_all_func(void *args);

class asyncfileio_t {

public:
    asyncfileio_t(std::string file_prefix) {
        malloc_stats();
        MallocExtension::instance()->ReleaseFreeMemory();
        malloc_stats();
        this->file_prefix = file_prefix;
        finish_thread_counter = new atomic<int>(0);
        barrier = new Barrier(SEND_THREAD_NUM);
        for (int i = 0; i < IO_THREAD; i++) {
            work_threads_object[i] = new asyncfileio_thread_t(i, file_prefix);
        }
    }

    void startIOThread() {
        for (int i = 0; i < IO_THREAD; i++) {
            work_threads_handle[i] = std::thread(ioThreadFunction, work_threads_object[i]);
            work_threads_handle[i].detach();
        }
    }


    void waitFinishIO(int tid) {
        if (!ioFinished) {
            if (tid == 0) {
                printf("in wait_flush function %ld\n", getCurrentTimeInMS());
            }
            barrier->Wait([this] {
                printf("start send flush cmd %ld\n", getCurrentTimeInMS());
                for (int i = 0; i < IO_THREAD; i++) {
                    asyncio_task_t *task = new asyncio_task_t(-1);
                    work_threads_object[i]->blockingQueue->put(task);
                }
                printf("after send flush cmd %ld\n", getCurrentTimeInMS());
            });
            if (tid == 0) {
                printf("before wait flush finish %ld\n", getCurrentTimeInMS());
            }
            while (finish_thread_counter->load() < IO_THREAD) {};
            if (tid == 0) {
                printf("after wait flush finish %ld\n", getCurrentTimeInMS());
            }
            barrier->Wait([this] {
//                malloc_stats();
                MallocExtension::instance()->ReleaseFreeMemory();
//                malloc_stats();
                for (int i = 0; i < IO_THREAD; i++) {
                    string tmp_str = file_prefix + "_" + std::to_string(i) + ".idx";
                    index_fds[i] = open(tmp_str.c_str(), O_RDONLY, S_IRUSR | S_IWUSR);
                    data_fds[i] = work_threads_object[i]->data_file_fd;
                    mapped_index_files_length[i] = work_threads_object[i]->index_file_size;

                    int ret = posix_fadvise(index_fds[i], 0,
                                            work_threads_object[i]->index_file_size,
                                            POSIX_FADV_RANDOM);
                    printf("ret %d\n", ret);
                }

            });
            if (tid == 0) {
                printf("finish wait_flush function %ld\n", getCurrentTimeInMS());
            }
            ioFinished = true;
        }
    }

    ~asyncfileio_t() {
        printf("f\n");
        std::thread flush_thread(flush_all_func, this);
        flush_thread.detach();
    }

    string file_prefix;

    asyncfileio_thread_t *work_threads_object[IO_THREAD];
    std::thread work_threads_handle[IO_THREAD];

    size_t mapped_index_files_length[IO_THREAD];

    int index_fds[IO_THREAD];
    int data_fds[IO_THREAD];

};

void flush_all_func(void *args) {
    asyncfileio_t *asyncfileio = (asyncfileio_t *) args;
    for (int tid = 0; tid < IO_THREAD; tid++) {
        asyncfileio_thread_t *work_thread = asyncfileio->work_threads_object[tid];
        for (size_t i = work_thread->index_mapped_flush_start_chunkID;
             i < MAX_MAPED_CHUNK_NUM && work_thread->index_file_memory_block[i] != nullptr; i++) {
            work_thread->index_mapped_flush_end_chunkID++;
            work_thread->index_file_size += INDEX_MAPPED_BLOCK_ALIGNED_SIZE;
            ftruncate(work_thread->index_file_fd, work_thread->index_file_size);

            pwrite(work_thread->index_file_fd, work_thread->index_file_memory_block[i],
                   INDEX_MAPPED_BLOCK_ALIGNED_SIZE, INDEX_MAPPED_BLOCK_ALIGNED_SIZE * i);
            free(work_thread->index_file_memory_block[i]);
        }
        fsync(work_thread->index_file_fd);
    }
}
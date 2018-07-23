#include <cstring>
#include <unistd.h>

#include "io/asyncfileio.hpp"
#include "../include/queue_store.h"
#include "utils/serialization.h"
#include "io/ConcurrentSet.hpp"


using namespace race2018;
using std::move;

atomic<int> tidCounter(-1);

void print_version() {
    printf("VERSION: %s\n", GIT_SHA1);
}

int64_t getQueueID(const std::string &queue_name) {
    long res = 0;
    long multiplier = 1;
    for (int64_t i = queue_name.length() - 1; i >= 0 && queue_name[i] >= '0' && queue_name[i] <= '9'; i--) {
        res += (queue_name[i] - '0') * multiplier;
        multiplier *= 10;
    }
    return res;
}

void queue_store::put(const std::string &queue_name, const MemBlock &message) {
    // 1st: queue id
    auto queueID = static_cast<unsigned long>(getQueueID(queue_name));
    auto thread_id = static_cast<int>(queueID % IO_THREAD);
    asyncfileio_thread_t *ioThread = asyncfileio->work_threads_object[thread_id];

    uint64_t which_queue_in_this_io_thread = queueID / IO_THREAD;
    uint64_t queue_offset = ioThread->queue_counter[which_queue_in_this_io_thread]++;
    uint64_t chunk_id = ((queue_offset / CLUSTER_SIZE) * (CLUSTER_SIZE * QUEUE_NUM_PER_IO_THREAD) +
                         (which_queue_in_this_io_thread * CLUSTER_SIZE) +
                         queue_offset % CLUSTER_SIZE);
    uint64_t idx_file_offset = INDEX_ENTRY_SIZE * chunk_id;

    int which_mapped_chunk = static_cast<int>(idx_file_offset / INDEX_MAPPED_BLOCK_RAW_SIZE);

    uint64_t offset_in_mapped_chunk = idx_file_offset % INDEX_MAPPED_BLOCK_RAW_SIZE;

    if (ioThread->index_file_memory_block[which_mapped_chunk] == nullptr) {
        std::unique_lock<std::mutex> lock(ioThread->mapped_block_mtx[which_mapped_chunk]);
        ioThread->mapped_block_cond[which_mapped_chunk].wait(lock, [ioThread, which_mapped_chunk]() -> bool {
            return ioThread->index_file_memory_block[which_mapped_chunk] != nullptr;
        });

    }
    char *buf = ioThread->index_file_memory_block[which_mapped_chunk] + offset_in_mapped_chunk;

    if (message.size <= RAW_NORMAL_MESSAGE_SIZE) {
        serialize_base36_decoding_skip_index((uint8_t *) message.ptr, message.size,
                                             (uint8_t *) buf);
    } else {
        unsigned char large_msg_buf[4096];
        uint64_t length = (uint64_t) serialize_base36_decoding_skip_index((uint8_t *) message.ptr, message.size,
                                                                          (uint8_t *) large_msg_buf);
        uint64_t offset = ioThread->data_file_current_size.fetch_add(length);
        pwrite(ioThread->data_file_fd, large_msg_buf, length, offset);

        // index info
        buf[0] = LARGE_MESSAGE_MAGIC_CHAR;
        memcpy(buf + 4, &offset, 8);
        memcpy(buf + 12, &length, 8);
    }

    delete[] ((char *) (message.ptr));

    int write_times = ++(ioThread->index_mapped_block_write_counter[which_mapped_chunk]);
    if (write_times == INDEX_BLOCK_WRITE_TIMES_TO_FULL) {
        asyncio_task_t *task = new asyncio_task_t(0);
        ioThread->blockingQueue->put(task);
    }
}

vector<MemBlock> queue_store::get(const std::string &queue_name, long offset, long number) {
    static thread_local int tid = ++tidCounter;
    if (tid < CHECK_THREAD_NUM) {
        return doPhase2(tid, queue_name, offset, number);
    }
    return doPhase3(tid, queue_name, offset, number);
}

std::vector<MemBlock> queue_store::doPhase2(int tid, const std::string &queue_name, long offset, long number) {
    asyncfileio->waitFinishIO(tid);

    static thread_local vector<MemBlock> result;

    result.clear();
    auto queueID = static_cast<unsigned long>(getQueueID(queue_name));
    int threadID = queueID % IO_THREAD;
    asyncfileio_thread_t *asyncfileio_thread = asyncfileio->work_threads_object[threadID];
    uint32_t which_queue_in_this_io_thread = queueID / IO_THREAD;

    auto max_offset = std::min(static_cast<uint32_t>(offset + number),
                               asyncfileio_thread->queue_counter[which_queue_in_this_io_thread]);

    uint64_t chunk_offset = (which_queue_in_this_io_thread * CLUSTER_SIZE);

    static thread_local unsigned char *index_record = (unsigned char *) memalign(FILESYSTEM_BLOCK_SIZE,
                                                                                 (INDEX_ENTRY_SIZE * CLUSTER_SIZE) +
                                                                                 FILESYSTEM_BLOCK_SIZE);
    for (auto queue_offset = static_cast<uint64_t>(offset); queue_offset < max_offset;) {
        uint64_t chunk_id = ((queue_offset / CLUSTER_SIZE) * (CLUSTER_SIZE * QUEUE_NUM_PER_IO_THREAD) + chunk_offset +
                             queue_offset % CLUSTER_SIZE);

        auto remaining_num = static_cast<uint32_t>(CLUSTER_SIZE - queue_offset % CLUSTER_SIZE);     // >= 1
        if (max_offset - queue_offset < remaining_num) {
            remaining_num = static_cast<uint32_t >(max_offset - queue_offset);
        }
        uint64_t idx_file_offset = INDEX_ENTRY_SIZE * chunk_id;
        idx_file_offset = (idx_file_offset / INDEX_MAPPED_BLOCK_RAW_SIZE * INDEX_MAPPED_BLOCK_ALIGNED_SIZE) +
                          (idx_file_offset % INDEX_MAPPED_BLOCK_RAW_SIZE);

        uint64_t idx_file_offset_aligned_start = (idx_file_offset / FILESYSTEM_BLOCK_SIZE * FILESYSTEM_BLOCK_SIZE);

        size_t which_mapped_chunk = idx_file_offset_aligned_start / INDEX_MAPPED_BLOCK_ALIGNED_SIZE;
        if (which_mapped_chunk < asyncfileio_thread->index_mapped_flush_start_chunkID) {
            pread(asyncfileio->index_fds[queueID % IO_THREAD], index_record,
                  ((INDEX_ENTRY_SIZE * remaining_num + (idx_file_offset - idx_file_offset_aligned_start)) /
                   FILESYSTEM_BLOCK_SIZE + 1) * FILESYSTEM_BLOCK_SIZE,
                  idx_file_offset_aligned_start);
        } else {
            memcpy(index_record,
                   asyncfileio_thread->index_file_memory_block[which_mapped_chunk] +
                   (idx_file_offset_aligned_start % INDEX_MAPPED_BLOCK_ALIGNED_SIZE),
                   (INDEX_ENTRY_SIZE * remaining_num) + (idx_file_offset - idx_file_offset_aligned_start));
        }

        for (uint32_t i = 0; i < remaining_num; i++) {
            char *output_buf = nullptr;
            int output_length;
            unsigned char *serialized =
                    index_record + INDEX_ENTRY_SIZE * i + idx_file_offset - idx_file_offset_aligned_start;
            if ((serialized[0] & 0xff) >> 2 != LARGE_MESSAGE_MAGIC_CHAR) {
                output_buf = (char *) deserialize_base36_encoding_add_index(serialized, INDEX_ENTRY_SIZE,
                                                                            output_length, queue_offset + i);
            } else {
                log_info("big msg");
                size_t large_msg_size;
                size_t large_msg_offset;
                memcpy(&large_msg_offset, serialized + 4, 8);
                memcpy(&large_msg_size, serialized + 12, 8);
                unsigned char large_msg_buf[4096];
                pread(asyncfileio->data_fds[queueID % IO_THREAD], large_msg_buf, large_msg_size,
                      large_msg_offset);
                output_buf = (char *) deserialize_base36_encoding_add_index((uint8_t *) large_msg_buf, large_msg_size,
                                                                            output_length, queue_offset + i);
            }
            result.emplace_back(output_buf, (size_t) output_length);
        }
        queue_offset += remaining_num;
    }

    return result;
}

volatile bool startedReaderThreadFlag = false;

std::vector<MemBlock> queue_store::doPhase3(int tid, const std::string &queue_name, long offset, long number) {
    auto queueID = static_cast<unsigned long>(getQueueID(queue_name));
    static thread_local vector<MemBlock> result;

    result.clear();

    int threadID = queueID % IO_THREAD;
    asyncfileio_thread_t *asyncfileio_thread = asyncfileio->work_threads_object[threadID];
    uint32_t which_queue_in_this_io_thread = queueID / IO_THREAD;

    size_t max_queue_offset = asyncfileio_thread->queue_counter[which_queue_in_this_io_thread];
    size_t max_result_num = 10 < (max_queue_offset - offset) ? 10 : (max_queue_offset - offset);

    if (offset == 0 && !startedReaderThreadFlag) {
        barrier1->Wait([this] {
            for (int i = 0; i < IO_THREAD; i++) {
//                for (size_t chunkID = asyncfileio->work_threads_object[i]->index_mapped_flush_start_chunkID;
//                     chunkID < asyncfileio->work_threads_object[i]->index_mapped_flush_end_chunkID; chunkID++) {
//                    //free(asyncfileio->work_threads_object[i]->index_file_memory_block[chunkID]);
//                    log_debug("free thread %d chunk id %ld", i, chunkID);
//                }
                //MallocExtension::instance()->ReleaseFreeMemory();
                close(asyncfileio->index_fds[i]);
                string tmp_str = asyncfileio->file_prefix + "_" + std::to_string(i) + ".idx";
                asyncfileio->index_fds[i] = open(tmp_str.c_str(), O_RDONLY | O_DIRECT, S_IRUSR | S_IWUSR);
//                posix_fadvise(asyncfileio->index_fds[i], 0,
//                              asyncfileio->mapped_index_files_length[i],
//                              POSIX_FADV_NORMAL);
            }
            printf("phase3 start\n");
        });
        startedReaderThreadFlag = true;
    }

    if (max_result_num <= 0) {
        return result;
    }

    static thread_local unsigned char **reader_hash_buffer = new unsigned char *[TOTAL_QUEUE_NUM]();
    static thread_local short *reader_hash_buffer_start_offset = new short[TOTAL_QUEUE_NUM]();

    if (reader_hash_buffer[queueID] == nullptr) {
        reader_hash_buffer[queueID] = (unsigned char *) memalign(FILESYSTEM_BLOCK_SIZE,
                                                                 (INDEX_ENTRY_SIZE * CLUSTER_SIZE) +
                                                                 FILESYSTEM_BLOCK_SIZE);
    }

    size_t read_num_left = max_result_num;

    for (size_t new_offset = offset; new_offset < offset + max_result_num;) {

        size_t this_max_read = std::min<size_t>(read_num_left, CLUSTER_SIZE - (new_offset % CLUSTER_SIZE));

        if (new_offset % CLUSTER_SIZE == 0) {
            uint64_t chunk_offset = (which_queue_in_this_io_thread * CLUSTER_SIZE);

            uint64_t chunk_id = ((new_offset / CLUSTER_SIZE) * (CLUSTER_SIZE * QUEUE_NUM_PER_IO_THREAD) + chunk_offset +
                                 new_offset % CLUSTER_SIZE);

            uint64_t idx_file_offset = INDEX_ENTRY_SIZE * chunk_id;

            idx_file_offset = (idx_file_offset / INDEX_MAPPED_BLOCK_RAW_SIZE * INDEX_MAPPED_BLOCK_ALIGNED_SIZE) +
                              (idx_file_offset % INDEX_MAPPED_BLOCK_RAW_SIZE);

            uint64_t idx_file_offset_aligned_start = (idx_file_offset / FILESYSTEM_BLOCK_SIZE * FILESYSTEM_BLOCK_SIZE);
            reader_hash_buffer_start_offset[queueID] = static_cast<short>(idx_file_offset -
                                                                          idx_file_offset_aligned_start);

            size_t which_mapped_chunk = idx_file_offset_aligned_start / INDEX_MAPPED_BLOCK_ALIGNED_SIZE;

            if (which_mapped_chunk < asyncfileio_thread->index_mapped_flush_start_chunkID) {
                pread(asyncfileio->index_fds[threadID], reader_hash_buffer[queueID],
                      ((INDEX_ENTRY_SIZE * CLUSTER_SIZE + (idx_file_offset - idx_file_offset_aligned_start)) /
                       FILESYSTEM_BLOCK_SIZE + 1) * FILESYSTEM_BLOCK_SIZE,
                      idx_file_offset_aligned_start);
            } else {
                memcpy(reader_hash_buffer[queueID],
                       asyncfileio_thread->index_file_memory_block[which_mapped_chunk] +
                       (idx_file_offset_aligned_start % INDEX_MAPPED_BLOCK_ALIGNED_SIZE),
                       (INDEX_ENTRY_SIZE * CLUSTER_SIZE) + (idx_file_offset - idx_file_offset_aligned_start));
            }

        }

        long in_cluster_offset = new_offset % CLUSTER_SIZE;

        for (uint32_t i = 0; i < this_max_read; i++) {
            char *output_buf = nullptr;
            int output_length;
            unsigned char *serialized = reader_hash_buffer[queueID] + INDEX_ENTRY_SIZE * (in_cluster_offset + i) +
                                        reader_hash_buffer_start_offset[queueID];
            if ((serialized[0] & 0xff) >> 2 != LARGE_MESSAGE_MAGIC_CHAR) {
                output_buf = (char *) deserialize_base36_encoding_add_index(serialized, INDEX_ENTRY_SIZE,
                                                                            output_length, new_offset + i);
            } else {
                size_t large_msg_size;
                size_t large_msg_offset;
                memcpy(&large_msg_offset, serialized + 4, 8);
                memcpy(&large_msg_size, serialized + 12, 8);
                unsigned char large_msg_buf[4096];
                pread(asyncfileio->data_fds[queueID % IO_THREAD], large_msg_buf, large_msg_size,
                      large_msg_offset);
                output_buf = (char *) deserialize_base36_encoding_add_index((uint8_t *) large_msg_buf, large_msg_size,
                                                                            output_length, new_offset + i);
            }
            result.emplace_back(output_buf, (size_t) output_length);
        }

        new_offset += this_max_read;
        read_num_left -= this_max_read;
    }

    return result;
}


queue_store::queue_store() {
    print_version();
    barrier1 = new Barrier(CHECK_THREAD_NUM);

    asyncfileio = new asyncfileio_t(DATA_FILE_PATH);
    asyncfileio->startIOThread();
}
#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <set>
#include <atomic>

#include "queue_store_config.h"

class asyncfileio_t;
class Barrier;

using std::string;
using std::vector;
using std::unordered_map;
using std::mutex;

void print_version();

namespace race2018 {

    struct MemBlock {
        /**
         * Pointer to the data
         */
        void *ptr;

        /**
         * Length of the data in bytes
         */
        size_t size;

        MemBlock(void* ptr, size_t size): ptr(ptr), size(size){
        }
    };

    class queue_store {
    public:

        /**
         * Default constructor is REQUIRED and will be used to initialize your implementation. You may modify it but
         * please make sure it exists.
         */
        queue_store();

        /**
         * Note: Competitors need to implement this function and it will be called concurrently.
         *
         * 把一条消息写入一个队列；
         * 这个接口需要是线程安全的，也即评测程序会并发调用该接口进行put；
         * 每个queue中的内容，按发送顺序存储消息（可以理解为Java中的List），同时每个消息会有一个索引，索引从0开始；
         * 不同queue中的内容，相互独立，互不影响；
         * @param queue_name 代表queue名字，如果是第一次put，则自动生产一个queue
         * @param message message，代表消息的内容，评测时内容会随机产生，大部分长度在64字节左右，会有少量消息在1k左右
         */
        void put(const std::string &queue_name, const MemBlock &message);

        /**
         * Note: Competitors need to implement this function and it will be called concurrently.
         *
         * 从一个队列中读出一批消息，读出的消息要按照发送顺序来；
         * 这个接口需要是线程安全的，也即评测程序会并发调用该接口进行get；
         * 返回的vector会被并发读，但不涉及写，因此只需要是线程读安全就可以了；
         * @param queue_name 代表队列的名字
         * @param offset 代表消息的在这个队列中的起始消息索引
         * @param num 代表读取的消息的条数，如果消息足够，则返回num条，否则只返回已有的消息即可;没有消息了，则返回一个空的集合
         */
        std::vector<MemBlock> get(const std::string &queue_name, long offset, long number);

        std::vector<MemBlock> doPhase2(int tid, const std::string &queue_name, long offset, long number);

        std::vector<MemBlock> doPhase3(int tid, const std::string &queue_name, long offset, long number);
    private:
        asyncfileio_t *asyncfileio;
        Barrier *barrier1;
    };
}
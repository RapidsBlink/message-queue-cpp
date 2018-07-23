#include <cstring>

#include <thread>
#include <random>
#include <algorithm>    // std::shuffle

#include "../include/queue_store.h"
#include "utils/utils.h"
#include "utils/log.h"

using namespace std::chrono;
using std::unordered_map;
using std::atomic_int;
using std::atomic;
using std::thread;
using std::mutex;
using std::unique_ptr;

using namespace race2018;

vector<mutex> putLockArr;

void Producer(queue_store *queueStore, int number, int64_t maxTimeStamp, int maxMsgNum,
              std::atomic<int64_t> *counter, unordered_map<string, atomic_int *> *queueCounter) {
    int64_t count;
    while ((count = counter->fetch_add(1, std::memory_order_seq_cst)) < maxMsgNum &&
           getCurrentTimeInMS() <= maxTimeStamp) {
        int32_t qid = static_cast<int32_t>(count % queueCounter->size());
        string queueName = "Queue-" + std::to_string(count % queueCounter->size());
        putLockArr[qid].lock();
        int counter_int = queueCounter->find(queueName)->second->fetch_add(1, std::memory_order_seq_cst);
        //log_info("%d", counter_int);
        string counter_str = std::to_string(counter_int);
        char *buf = new char[counter_str.length() + 1];
        strcpy(buf, counter_str.c_str());
        MemBlock msg = {static_cast<void *>(buf),
                        static_cast<size_t>(counter_str.length())
        };
        queueStore->put(queueName, msg);
        putLockArr[qid].unlock();
    }
}

void IndexChecker(queue_store *queueStore, int number, int64_t maxTimeStamp, int maxMsgNum,
                  std::atomic<int64_t> *counter, unordered_map<string, atomic_int *> *queueCounter) {
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, queueCounter->size() - 1);
    std::uniform_int_distribution<int> queue_distribution[queueCounter->size()];
    for (unsigned long i = 0; i < queueCounter->size(); i++) {
        string queueName = "Queue-" + std::to_string(i);
        int counter_int = queueCounter->find(queueName)->second->load();
        queue_distribution[i] = std::uniform_int_distribution<int>(0, counter_int - 1);
    }
    while (counter->fetch_add(1, std::memory_order_seq_cst) < maxMsgNum && getCurrentTimeInMS() <= maxTimeStamp) {
        int queueID = distribution(generator);
        string queueName = "Queue-" + std::to_string(queueID);
        int index = queue_distribution[queueID](generator) - 10;
        if (index < 0) index = 0;
        vector<MemBlock> msgs = queueStore->get(queueName, index, 10);
        for (auto &msg: msgs) {
            string got_str = string((char *) msg.ptr, msg.size);
            string expected_str = std::to_string(index++);
            delete[] (char *) msg.ptr;
            if (got_str != expected_str) {
                log_error("check failed, got %s, expected: %s", got_str.c_str(), expected_str.c_str());
                exit(-1);
            }
        }
    }
}

void Consumer(queue_store *queueStore, int number, int64_t maxTimeStamp, std::atomic<int64_t> *counter,
              unordered_map<string, int> *queueCheckNames) {
    unordered_map<string, int *> pullOffsets;
    for (auto pair: *queueCheckNames) {
        int *count = static_cast<int *>(malloc(sizeof(int)));
        *count = 0;
        pullOffsets.insert(std::make_pair(pair.first, count));
        //log_info("%s %d", pair.first.c_str(), pair.second);
    }

    while (!pullOffsets.empty() && getCurrentTimeInMS() < maxTimeStamp) {
        for (auto pair = pullOffsets.begin(); pair != pullOffsets.end();) {
            string queueName = pair->first;
            int *count = pair->second;
            int index = *count;
            vector<MemBlock> msgs = queueStore->get(queueName, index, 10);
            //log_info("queueName %s msgs size %d", queueName.c_str(), msgs.size());
            if (msgs.size() > 0) {
                *count += msgs.size();
                for (auto &msg: msgs) {
                    string got_str = string((char *) msg.ptr, msg.size);
                    string expected_str = std::to_string(index++);
                    delete[] (char *) msg.ptr;
                    if (got_str != expected_str) {
                        log_error("check failed, got %s, expected: %s", got_str.c_str(), expected_str.c_str());
                        exit(-1);
                    }
                }
                counter->fetch_add(msgs.size(), std::memory_order_seq_cst);
            }
            if (msgs.size() < 10) {
                int got = *count;
                int expected = queueCheckNames->find(queueName)->second;
                if (got != expected) {
                    log_error("Queue Number Error, got %d, expected: %d", got, expected);
                    exit(-1);
                }
                pair = pullOffsets.erase(pair);
            } else {
                pair++;
            }
        }
    }
}

int main(int argc, char **argv) {
    //评测相关配置
    //发送阶段的发送数量，也即发送阶段必须要在规定时间内把这些消息发送完毕方可
//    int msgNum = 10000;
    int msgNum = 100000000;
//    int msgNum = 2000000000;
    //发送阶段的最大持续时间，也即在该时间内，如果消息依然没有发送完毕，则退出评测
    int sendTime = 10 * 60 * 1000;
    //消费阶段的最大持续时间，也即在该时间内，如果消息依然没有消费完毕，则退出评测
    int checkTime = 10 * 60 * 1000;
    //队列的数量
    int queueNum = 10000;
    //正确性检测的次数
    int checkNum = 1000;
    //消费阶段的总队列数量
    int checkQueueNum = 100;
    //发送的线程数量
    int sendTsNum = 10;
    //消费的线程数量
    int checkTsNum = 10;

    std::vector<std::mutex> list(queueNum);
    putLockArr.swap(list);

    auto *queueNumMap = new unordered_map<string, atomic_int *>;
    for (int i = 0; i < queueNum; i++) {
        queueNumMap->insert(std::make_pair<string, atomic_int *>("Queue-" + std::to_string(i), new atomic_int(0)));
    }


    auto *queueStore = new queue_store();

    //Step1: 发送消息
    int64_t sendStart = getCurrentTimeInMS();
    int64_t maxTimeStamp = sendStart + sendTime;

    auto *sendCounter = new std::atomic<int64_t>(0);

    thread sends[sendTsNum];
    for (int i = 0; i < sendTsNum; i++) {
        sends[i] = std::move(thread(Producer, queueStore, i, maxTimeStamp, msgNum, sendCounter, queueNumMap));
    }

    for (int i = 0; i < sendTsNum; i++) {
        sends[i].join();
    }

    int64_t sendSend = getCurrentTimeInMS();
    log_info("Send: %d ms Num:%d\n", sendSend - sendStart, sendCounter->load());

    //Step2: 索引的正确性校验
    int64_t indexCheckStart = getCurrentTimeInMS();
    int64_t maxCheckTime = indexCheckStart + checkTime;
    auto *indexCheckCounter = new std::atomic<int64_t>(0);

    thread indexChecks[sendTsNum];
    for (int i = 0; i < sendTsNum; i++) {
        indexChecks[i] = std::move(
                thread(IndexChecker, queueStore, i, maxCheckTime, checkNum, indexCheckCounter, queueNumMap));
    }

    for (int i = 0; i < sendTsNum; i++) {
        indexChecks[i].join();
    }
    int64_t indexCheckEnd = getCurrentTimeInMS();
    log_info("Index Check: %d ms Num:%d\n", indexCheckEnd - indexCheckStart,
             indexCheckCounter->load());

    //Step3: 消费消息，并验证顺序性
    int64_t checkStart = getCurrentTimeInMS();
    auto *checkCounter = new std::atomic<int64_t>(0);

    thread checks[sendTsNum];
    std::default_random_engine generator;
    std::uniform_int_distribution<int> distribution(0, queueNum - 1);

    vector<string> allQueueName;
    for (int i = 0; i < queueNum; i++) {
        allQueueName.push_back("Queue-" + std::to_string(i));
    }
    std::shuffle(allQueueName.begin(), allQueueName.end(),
                 std::default_random_engine((unsigned long long) getCurrentTimeInMS()));

    for (int i = 0; i < sendTsNum; i++) {
        int eachCheckQueueNum = checkQueueNum / checkTsNum;
        int myCheckQueueNumEnd = (i + 1) * eachCheckQueueNum;
        if (i == sendTsNum - 1) {
            myCheckQueueNumEnd = checkQueueNum;
        }
        auto *myCheckQueueName = new unordered_map<string, int>;
        for (int j = i * eachCheckQueueNum; j < myCheckQueueNumEnd; j++) {
            myCheckQueueName->insert(
                    std::make_pair(allQueueName[j], queueNumMap->find(allQueueName[j])->second->load()));
        }
        checks[i] = std::move(thread(Consumer, queueStore, i, maxCheckTime, checkCounter, myCheckQueueName));
    }
    for (int i = 0; i < sendTsNum; i++) {
        checks[i].join();
    }
    long checkEnd = getCurrentTimeInMS();
    log_info("Check: %d ms Num: %d\n", checkEnd - checkStart, checkCounter->load());

    //评测结果
    log_info("Tps:%f\n", ((sendCounter->load() + checkCounter->load() + indexCheckCounter->load()) + 0.1) * 1000 /
                         ((sendSend - sendStart) + (checkEnd - checkStart) + (indexCheckEnd - indexCheckStart)));

    return 0;
}
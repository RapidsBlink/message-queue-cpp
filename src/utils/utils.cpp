#include "utils.h"

#include <chrono>
#include <cstdio>
#include <cassert>

int64_t getCurrentTimeInMS() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
}

void save_to_binary_file(char *filename, char *buf, int size) {
    assert(size > 0);
    printf("save to binary size %d\n", size);
    FILE *ptr = fopen(filename, "wb");
    fwrite(buf, sizeof(char), size, ptr);
    fflush(ptr);
    fclose(ptr);
}

char *read_binary_file(char *filename, size_t *size) {
    FILE *ptr = fopen(filename, "rb");
    fseek(ptr, 0, SEEK_END); // seek to end of file
    *size = ftell(ptr); // get current file pointer
    fseek(ptr, 0, SEEK_SET); // seek back to beginning of file
    char *buf = (char *) malloc(sizeof(char) * (*size));
    size_t ret = fread(buf, sizeof(char), *size, ptr);
    printf("read binary size %ld\n", ret);
    return buf;
}

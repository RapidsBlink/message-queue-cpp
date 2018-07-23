#pragma once


#ifdef __cplusplus

#include <cstdint>
#include <cstdlib>

extern "C" {
#else
#include <stdint.h>
#include <stdlib.h>
#endif

int64_t getCurrentTimeInMS();

char *read_binary_file(char *filename, size_t *size);

void save_to_binary_file(char *filename, char *buf, int size);

#ifdef __cplusplus
}
#endif
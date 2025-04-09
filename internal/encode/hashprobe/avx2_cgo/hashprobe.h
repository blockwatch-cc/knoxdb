#ifndef HASHPROBE_H
#define HASHPROBE_H

#include <stdint.h>
#include <stddef.h>

void ht_build64(uint64_t* vals, uint64_t* ht_keys, uint16_t* ht_values, uint64_t* dict, size_t len, size_t* dict_size);
void ht_build32(uint32_t* vals, uint32_t* ht_keys, uint16_t* ht_values, uint32_t* dict, size_t len, size_t* dict_size);

void ht_encode64(uint64_t* vals, uint64_t* ht_keys, uint16_t* ht_values, uint16_t* codes, size_t len);
void ht_encode32(uint32_t* vals, uint32_t* ht_keys, uint16_t* ht_values, uint16_t* codes, size_t len);

#endif
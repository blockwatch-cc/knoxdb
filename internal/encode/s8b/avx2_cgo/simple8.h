#ifndef SIMPLE8_H
#define SIMPLE8_H

#include <stdint.h>
#include <stddef.h>

size_t encode_u64_avx2(uint8_t* dst, const uint64_t* src, size_t len, uint64_t minv);
size_t encode_u32_avx2(uint8_t* dst, const uint32_t* src, size_t len, uint32_t minv);
size_t encode_u16_avx2(uint8_t* dst, const uint16_t* src, size_t len, uint16_t minv);
size_t encode_u8_avx2(uint8_t* dst, const uint8_t* src, size_t len, uint8_t minv);


#endif
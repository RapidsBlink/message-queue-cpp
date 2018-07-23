#include <cstdint>
#include <cstring>

#include "serialization.h"


#include "../dependencies/fast_base64/fastavxbase64.h"
#include "log.h"

int serialize_base64_decoding(uint8_t *message, uint16_t len, uint8_t *serialized) {
    auto serialize_len = len - FIXED_PART_LEN;
    int padding_chars = (4 - serialize_len % 4) % 4;
    uint8_t *buf = message;

    size_t estimated_length = 3 * (serialize_len / 4 + (serialize_len % 4 == 0 ? 0 : 1));
    memcpy(serialized + estimated_length, message + serialize_len, FIXED_PART_LEN);
    // attention: add padding chars, assume following chars enough >= 3
    memcpy(message + serialize_len, "BLINK", padding_chars);

#ifdef __AVX2__
    fast_avx2_base64_decode(reinterpret_cast<char *>(serialized),
                                            reinterpret_cast<const char *>(buf),
                                            serialize_len + padding_chars);
#else

    chromium_base64_decode(reinterpret_cast<char *>(serialized),
                           reinterpret_cast<const char *>(buf),
                           serialize_len + padding_chars);
#endif
    serialized[estimated_length + FIXED_PART_LEN] = padding_chars;
    return estimated_length + FIXED_PART_LEN + 1;
}

uint8_t *deserialize_base64_encoding(const uint8_t *serialized, uint16_t total_serialized_len, int &len) {
    auto serialize_len = total_serialized_len - FIXED_PART_LEN - 1;
    auto *deserialized = new uint8_t[total_serialized_len / 3 * 4 + 16];

#ifdef __AVX2__
    size_t length = fast_avx2_base64_encode(reinterpret_cast<char *>(deserialized),
                                            reinterpret_cast<const char *>(serialized), serialize_len);
#else
    size_t length = chromium_base64_encode(reinterpret_cast<char *>(deserialized),
                                           reinterpret_cast<const char *>(serialized), serialize_len);

#endif
    memcpy(deserialized + length - serialized[total_serialized_len - 1], serialized + serialize_len, FIXED_PART_LEN);
    len = length - serialized[total_serialized_len - 1] + FIXED_PART_LEN;
    return deserialized;
}

// Skip index =================================================================================================
int serialize_base64_decoding_skip_index(uint8_t *message, uint16_t len, uint8_t *serialized) {
    auto serialize_len = len - FIXED_PART_LEN;
    int padding_chars = (4 - serialize_len % 4) % 4;
    uint8_t *buf = message;

    size_t estimated_length = 3 * (serialize_len / 4 + (serialize_len % 4 == 0 ? 0 : 1));
    memcpy(serialized + estimated_length, message + serialize_len, BASE64_INFO_LEN);
    memcpy(serialized + estimated_length + BASE64_INFO_LEN, message + serialize_len + BASE64_INFO_LEN + INDEX_LEN,
           VARYING_VERIFY_LEN);
    // attention: add padding chars, assume following chars enough >= 3
    memcpy(message + serialize_len, "BLINK", padding_chars);

#ifdef __AVX2__
    fast_avx2_base64_decode(reinterpret_cast<char *>(serialized),
                                            reinterpret_cast<const char *>(buf),
                                            serialize_len + padding_chars);
#else

    chromium_base64_decode(reinterpret_cast<char *>(serialized),
                           reinterpret_cast<const char *>(buf),
                           serialize_len + padding_chars);
#endif
    serialized[estimated_length + FIXED_PART_LEN - INDEX_LEN] = padding_chars;
    return estimated_length + FIXED_PART_LEN - INDEX_LEN + 1;
}

uint8_t *deserialize_base64_encoding_add_index(const uint8_t *serialized, uint16_t total_serialized_len,
                                               int &deserialized_len, int32_t idx) {
    auto serialize_len = total_serialized_len - (FIXED_PART_LEN - INDEX_LEN) - 1;
    auto *deserialized = new uint8_t[total_serialized_len / 3 * 4 + 16];

#ifdef __AVX2__
    size_t length = fast_avx2_base64_encode(reinterpret_cast<char *>(deserialized),
                                            reinterpret_cast<const char *>(serialized), serialize_len);
#else
    size_t length = chromium_base64_encode(reinterpret_cast<char *>(deserialized),
                                           reinterpret_cast<const char *>(serialized), serialize_len);

#endif
    size_t offset = length - serialized[total_serialized_len - 1];
    memcpy(deserialized + offset, serialized + serialize_len, BASE64_INFO_LEN);
    offset += BASE64_INFO_LEN;
    memcpy(deserialized + offset, &idx, sizeof(int32_t));
    offset += INDEX_LEN;
    memcpy(deserialized + offset, serialized + serialize_len + BASE64_INFO_LEN, VARYING_VERIFY_LEN);

    deserialized_len = length - serialized[total_serialized_len - 1] + FIXED_PART_LEN;
    return deserialized;
}

void deserialize_base64_encoding_add_index_in_place(const uint8_t *serialized, uint16_t total_serialized_len,
                                                    uint8_t *deserialized, int &deserialized_len, int32_t idx) {
    auto serialize_len = total_serialized_len - (FIXED_PART_LEN - INDEX_LEN) - 1;

#ifdef __AVX2__
    size_t length = fast_avx2_base64_encode(reinterpret_cast<char *>(deserialized),
                                            reinterpret_cast<const char *>(serialized), serialize_len);
#else
    size_t length = chromium_base64_encode(reinterpret_cast<char *>(deserialized),
                                           reinterpret_cast<const char *>(serialized), serialize_len);

#endif
    size_t offset = length - serialized[total_serialized_len - 1];
    memcpy(deserialized + offset, serialized + serialize_len, BASE64_INFO_LEN);
    offset += BASE64_INFO_LEN;
    memcpy(deserialized + offset, &idx, sizeof(int32_t));
    offset += INDEX_LEN;
    memcpy(deserialized + offset, serialized + serialize_len + BASE64_INFO_LEN, VARYING_VERIFY_LEN);

    deserialized_len = length - serialized[total_serialized_len - 1] + FIXED_PART_LEN;
}
// End of Skip index ========================================================================================

// ------------------------------- Begin of Base36 -------------------------------------------------------------
int serialize_base36_decoding_skip_index(uint8_t *message, uint16_t len, uint8_t *serialized) {
    auto serialize_len = len - FIXED_PART_LEN;
    int padding_chars = (4 - serialize_len % 4) % 4;
    uint8_t *buf = message;

    size_t estimated_length = 3 * (serialize_len / 4 + (serialize_len % 4 == 0 ? 0 : 1));
    memcpy(serialized + estimated_length, message + serialize_len, BASE64_INFO_LEN);
    memcpy(serialized + estimated_length + BASE64_INFO_LEN, message + serialize_len + BASE64_INFO_LEN + INDEX_LEN,
           VARYING_VERIFY_LEN);
    // attention: add padding chars, assume following chars enough >= 3
    memcpy(message + serialize_len, "BLINK", padding_chars);

#ifdef __AVX2__
    fast_avx2_base64_decode(reinterpret_cast<char *>(serialized),
                                            reinterpret_cast<const char *>(buf),
                                            serialize_len + padding_chars);
#else

    chromium_base64_decode(reinterpret_cast<char *>(serialized),
                           reinterpret_cast<const char *>(buf),
                           serialize_len + padding_chars);
#endif
    return estimated_length + FIXED_PART_LEN - INDEX_LEN;
}

uint8_t *deserialize_base36_encoding_add_index(const uint8_t *serialized, uint16_t total_serialized_len,
                                               int &deserialized_len, int32_t idx) {
    auto serialize_len = total_serialized_len - (FIXED_PART_LEN - INDEX_LEN);
    auto *deserialized = new uint8_t[total_serialized_len / 3 * 4 + 16];
    // 1st: deserialize preparation: base64 encoding
#ifdef __AVX2__
    size_t length = fast_avx2_base64_encode(reinterpret_cast<char *>(deserialized),
                                            reinterpret_cast<const char *>(serialized), serialize_len);
#else
    size_t length = chromium_base64_encode(reinterpret_cast<char *>(deserialized),
                                           reinterpret_cast<const char *>(serialized), serialize_len);

#endif
    // 2nd: skip padding (padding could be 'A'-'Z', '+', '/', '=')
    for (; deserialized[length - 1] >= 'A' && deserialized[length - 1] <= 'Z' && length >= 0; length--) {}

    // 3rd: append other info
    size_t offset = length;
    memcpy(deserialized + offset, serialized + serialize_len, BASE64_INFO_LEN);
    offset += BASE64_INFO_LEN;
    memcpy(deserialized + offset, &idx, sizeof(int32_t));
    offset += INDEX_LEN;
    memcpy(deserialized + offset, serialized + serialize_len + BASE64_INFO_LEN, VARYING_VERIFY_LEN);

    // 4th: assign the correct length
    deserialized_len = length + FIXED_PART_LEN;
    return deserialized;
}
// ------------------------------ End of Base36, do not support A-Z yet --------------------------------------------

int serialize(uint8_t *message, uint16_t len, uint8_t *serialized) {
    // add the header to indicate raw message varying-length part size
    int serialize_len = len - FIXED_PART_LEN;
    if (len < 128) {
        serialized[0] = static_cast<uint8_t>(len - FIXED_PART_LEN);
        serialized += 1;
    } else {
        uint16_t tmp = (len - FIXED_PART_LEN);
        serialized[0] = static_cast<uint8_t>((tmp >> 7u) | 0x80); // assume < 32767
        serialized[1] = static_cast<uint8_t>(tmp & (uint8_t) 0x7f);
        serialized += 2;
    }
    uint32_t next_extra_3bits_idx = 5u * serialize_len;
    uint32_t next_5bits_idx = 0;

    // attention: message is not usable later
    for (int i = 0; i < serialize_len; i++) {
        message[i] = message[i] >= 'a' ? message[i] - 'a' : message[i] - '0' + (uint8_t) 26;
    }
    // attention: must clear to be correct
    memset(serialized, 0, (len - FIXED_PART_LEN));
    // 1) construct the compressed part
    for (int i = 0; i < serialize_len; i++) {
        uint16_t cur_uchar = message[i];
        uint16_t expand_uchar = cur_uchar < MAX_FIVE_BITS_INT ? (cur_uchar << 11u) : (MAX_FIVE_BITS_INT << 11u);

        int shift_bits = (next_5bits_idx & 0x7u);
        expand_uchar >>= shift_bits;
        int idx = (next_5bits_idx >> 3u);
        serialized[idx] |= (expand_uchar >> 8u);
        serialized[idx + 1] |= (expand_uchar & 0xffu);
        next_5bits_idx += 5;

        if (cur_uchar >= MAX_FIVE_BITS_INT) {
            // do extra bits operations
            expand_uchar = ((cur_uchar - MAX_FIVE_BITS_INT) << 13u);
            shift_bits = (next_extra_3bits_idx & 0x7u);
            expand_uchar >>= shift_bits;
            // assume little-endian
            idx = (next_extra_3bits_idx >> 3u);
            serialized[idx] |= (expand_uchar >> 8u);
            serialized[idx + 1] |= (expand_uchar & 0xffu);
            next_extra_3bits_idx += 3;
        }
    }

    // 2) left FIXED_PART_LEN, should use memcpy
    int start_copy_byte_idx = (next_extra_3bits_idx >> 3u) + ((next_extra_3bits_idx & 0x7u) != 0);
    memcpy(serialized + start_copy_byte_idx, message + serialize_len, FIXED_PART_LEN);
    return start_copy_byte_idx + FIXED_PART_LEN + (len < 128 ? 1 : 2);
}

uint8_t *deserialize(const uint8_t *serialized, int &len) {
    // get the length of varying part
    uint16_t varying_byte_len;
    if ((serialized[0] & 0x80u) == 0) {
        varying_byte_len = serialized[0];
        serialized += 1;
    } else {
        varying_byte_len = static_cast<uint16_t>(((serialized[0] & 0x7fu) << 7u) + serialized[1]);
        serialized += 2;
    }
    uint32_t next_extra_3bits_idx = 5u * varying_byte_len;
    uint32_t next_5bits_idx = 0;

    auto *deserialized = new uint8_t[varying_byte_len + 8];
    len = varying_byte_len + FIXED_PART_LEN;
    // deserialize
    for (int i = 0; i < varying_byte_len; i++) {
        int idx = (next_5bits_idx >> 3u);
        uint16_t value = (serialized[idx] << 8u) + serialized[idx + 1];
        value = (value >> (11u - (next_5bits_idx & 07u))) & MAX_FIVE_BITS_INT;
        if (value != MAX_FIVE_BITS_INT) {
            deserialized[i] = static_cast<uint8_t>(value < 26 ? 'a' + value : value - 26 + '0');
        } else {
            idx = (next_extra_3bits_idx >> 3u);
            value = (serialized[idx] << 8u) + serialized[idx + 1];
            value = (value >> (13u - (next_extra_3bits_idx & 07u))) & (uint8_t) 0x7;
            deserialized[i] = value + '5';
            next_extra_3bits_idx += 3;
        }
        next_5bits_idx += 5;
    }

    // 2) copy the fixed part
    memcpy(deserialized + varying_byte_len,
           serialized + (next_extra_3bits_idx >> 3u) + ((next_extra_3bits_idx & 0x7u) != 0), FIXED_PART_LEN);
    return deserialized;
}
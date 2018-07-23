#ifndef QUEUERACE_SERIALIZATION_H
#define QUEUERACE_SERIALIZATION_H

#define BASE64_INFO_LEN (2u)
#define INDEX_LEN (4u)
#define VARYING_VERIFY_LEN (4u)
#define FIXED_PART_LEN (10u)

#define MAX_FIVE_BITS_INT ((uint8_t) 0x1f)      // 31

int serialize(uint8_t *message, uint16_t len, uint8_t *serialized);

uint8_t *deserialize(const uint8_t *serialized, int &len);

int serialize_base64_decoding(uint8_t *message, uint16_t len, uint8_t *serialized);

uint8_t *deserialize_base64_encoding(const uint8_t *serialized, uint16_t total_serialized_len, int &len);

// ================= skip index ===================================================================
int serialize_base64_decoding_skip_index(uint8_t *message, uint16_t len, uint8_t *serialized);

// attention: new[] inside
uint8_t *deserialize_base64_encoding_add_index(const uint8_t *serialized, uint16_t total_serialized_len,
                                               int &deserialized_len, int32_t idx);

// not able to use in the current testing environment
void deserialize_base64_encoding_add_index_in_place(const uint8_t *serialized, uint16_t total_serialized_len,
                                                    uint8_t *deserialized, int &deserialized_len, int32_t idx);

// ======================================================= end of base64

// =========================== start of base36 ============================================

int serialize_base36_decoding_skip_index(uint8_t *message, uint16_t len, uint8_t *serialized);

uint8_t *deserialize_base36_encoding_add_index(const uint8_t *serialized, uint16_t total_serialized_len,
                                               int &deserialized_len, int32_t idx);

// ============================ end of base36 =============================================

#endif //QUEUERACE_SERIALIZATION_H

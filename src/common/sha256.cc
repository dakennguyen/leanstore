#include "common/sha256.h"

namespace leanstore {

SHA256H::SHA256H() { Initialize(); }

void SHA256H::Initialize() {
  h0145        = _mm_set_epi32(H0, H1, H4, H5);
  h2367        = _mm_set_epi32(H2, H3, H6, H7);
  msgbuf_count = 0;
  total_count  = 0;
}

void SHA256H::Update(const void *buf, size_t length) {
  const unsigned char *p = (const unsigned char *)buf;
  total_count += length;

  // If any bytes are left in the message buffer,
  // fullfill the block first
  if (msgbuf_count) {
    size_t c = MBYTES - msgbuf_count;
    if (length < c) {
      memcpy(msgbuf + msgbuf_count, p, length);
      msgbuf_count += length;
      return;
    } else {
      memcpy(msgbuf + msgbuf_count, p, c);
      p += c;
      length -= c;
      ProcessMsgBlock(msgbuf);
      msgbuf_count = 0;
    }
  }

  // When we reach here, we have no data left in the message buffer
  while (length >= MBYTES) {
    // No need to copy into the internal message block
    ProcessMsgBlock(p);
    p += MBYTES;
    length -= MBYTES;
  }

  // Leave the remaining bytes in the message buffer
  if (length) {
    memcpy(msgbuf, p, length);
    msgbuf_count = length;
  }
}

void SHA256H::Serialize(uint64_t *s) {
  std::memcpy(s, &h0145, sizeof(h0145));
  std::memcpy(&s[2], &h2367, sizeof(h2367));
}

void SHA256H::Final(void *digest) {
  // Add the terminating bit
  msgbuf[msgbuf_count++] = 0x80;

  // Need to set total length in the last 8-byte of the block.
  // If there is no room for the length, process this block first
  if (msgbuf_count + 8 > MBYTES) {
    // Fill zeros and process
    memset(msgbuf + msgbuf_count, 0, MBYTES - msgbuf_count);
    ProcessMsgBlock(msgbuf);
    msgbuf_count = 0;
  }

  // Fill zeros before the last 8-byte of the block
  memset(msgbuf + msgbuf_count, 0, MBYTES - 8 - msgbuf_count);

  // Set the length of the message in big-endian
  __m128i tmp                             = _mm_loadl_epi64((__m128i *)&total_count);
  tmp                                     = _mm_slli_epi64(tmp, 3);  // convert # of bytes to # of bits
  const __m128i total_count_byteswapindex = _mm_set_epi8(-1, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4, 5, 6, 7);
  tmp                                     = _mm_shuffle_epi8(tmp, total_count_byteswapindex);  // convert to big endian
  _mm_storel_epi64((__m128i *)(msgbuf + MBYTES - 8), tmp);

  // Process the last block
  ProcessMsgBlock(msgbuf);

  // Get the resulting hash value.
  // h0:h1:h4:h5
  // h2:h3:h6:h7
  //      |
  //      V
  // h0:h1:h2:h3
  // h4:h5:h6:h7
  __m128i h0123 = _mm_unpackhi_epi64(h2367, h0145);
  __m128i h4567 = _mm_unpacklo_epi64(h2367, h0145);

  // Swap the byte order
  const __m128i byteswapindex = _mm_set_epi8(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

  h0123 = _mm_shuffle_epi8(h0123, byteswapindex);
  h4567 = _mm_shuffle_epi8(h4567, byteswapindex);

  __m128i *digestX = (__m128i *)digest;
  _mm_storeu_si128(digestX, h0123);
  _mm_storeu_si128(digestX + 1, h4567);
}

void SHA256H::ProcessMsgBlock(const unsigned char *msg) {
  // Cyclic W array
  // We keep the W array content cyclically in 4 variables
  // Initially:
  // cw0 = w3 : w2 : w1 : w0
  // cw1 = w7 : w6 : w5 : w4
  // cw2 = w11 : w10 : w9 : w8
  // cw3 = w15 : w14 : w13 : w12
  const __m128i byteswapindex = _mm_set_epi8(12, 13, 14, 15, 8, 9, 10, 11, 4, 5, 6, 7, 0, 1, 2, 3);
  const __m128i *msgx         = (const __m128i *)msg;
  __m128i cw0                 = _mm_shuffle_epi8(_mm_loadu_si128(msgx), byteswapindex);
  __m128i cw1                 = _mm_shuffle_epi8(_mm_loadu_si128(msgx + 1), byteswapindex);
  __m128i cw2                 = _mm_shuffle_epi8(_mm_loadu_si128(msgx + 2), byteswapindex);
  __m128i cw3                 = _mm_shuffle_epi8(_mm_loadu_si128(msgx + 3), byteswapindex);

// Advance W array cycle
// Inputs:
//  CW0 = w[t-13] : w[t-14] : w[t-15] : w[t-16]
//  CW1 = w[t-9] : w[t-10] : w[t-11] : w[t-12]
//  CW2 = w[t-5] : w[t-6] : w[t-7] : w[t-8]
//  CW3 = w[t-1] : w[t-2] : w[t-3] : w[t-4]
// Outputs:
//  CW1 = w[t-9] : w[t-10] : w[t-11] : w[t-12]
//  CW2 = w[t-5] : w[t-6] : w[t-7] : w[t-8]
//  CW3 = w[t-1] : w[t-2] : w[t-3] : w[t-4]
//  CW0 = w[t+3] : w[t+2] : w[t+1] : w[t]
#define CYCLE_W(CW0, CW1, CW2, CW3)                                                            \
  CW0 = _mm_sha256msg1_epu32(CW0, CW1);                                                        \
  CW0 = _mm_add_epi32(CW0, _mm_alignr_epi8(CW3, CW2, 4)); /* add w[t-4]:w[t-5]:w[t-6]:w[t-7]*/ \
  CW0 = _mm_sha256msg2_epu32(CW0, CW3);

  __m128i state1 = h0145;  // a:b:e:f
  __m128i state2 = h2367;  // c:d:g:h

  // Add to the intermediate hash
  h0145 = _mm_add_epi32(state1, h0145);
  h2367 = _mm_add_epi32(state2, h2367);
}

}  // namespace leanstore

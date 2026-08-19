// FIPS 180-4 SHA-256 (https://doi.org/10.6028/NIST.FIPS.180-4), section 6.2. Used for the
// ACL password digest Redis prints as "#<hex>" (acl.c ACLHashPassword). Deliberately not
// the SHA-1 already in the tree as sha1_hex (src/command.cpp) — that is a correct SHA-1,
// not SHA-256, and is the wrong algorithm for this.

#include "sha256.hpp"

#include <array>
#include <cstdint>
#include <cstring>

namespace peadb {
namespace {

// FIPS 180-4 section 4.2.2: the first 32 bits of the fractional parts of the cube roots
// of the first 64 primes.
constexpr std::array<uint32_t, 64> K = {
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
    0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
    0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
    0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
    0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
    0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
    0xc67178f2,
};

inline uint32_t rotr(uint32_t x, unsigned n) { return (x >> n) | (x << (32 - n)); }

inline uint32_t choose(uint32_t e, uint32_t f, uint32_t g) { return (e & f) ^ (~e & g); }

inline uint32_t majority(uint32_t a, uint32_t b, uint32_t c) {
  return (a & b) ^ (a & c) ^ (b & c);
}

// Big Sigma / small sigma functions, FIPS 180-4 section 4.1.2.
inline uint32_t bigSigma0(uint32_t a) { return rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22); }
inline uint32_t bigSigma1(uint32_t e) { return rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25); }
inline uint32_t smallSigma0(uint32_t x) { return rotr(x, 7) ^ rotr(x, 18) ^ (x >> 3); }
inline uint32_t smallSigma1(uint32_t x) { return rotr(x, 17) ^ rotr(x, 19) ^ (x >> 10); }

// Processes one 64-byte block, updating the running hash state h[0..7] in place.
void processBlock(std::array<uint32_t, 8>& h, const uint8_t block[64]) {
  std::array<uint32_t, 64> w{};
  for (int i = 0; i < 16; ++i) {
    w[i] = (static_cast<uint32_t>(block[i * 4]) << 24) |
           (static_cast<uint32_t>(block[i * 4 + 1]) << 16) |
           (static_cast<uint32_t>(block[i * 4 + 2]) << 8) |
           static_cast<uint32_t>(block[i * 4 + 3]);
  }
  for (int i = 16; i < 64; ++i) {
    w[i] = smallSigma1(w[i - 2]) + w[i - 7] + smallSigma0(w[i - 15]) + w[i - 16];
  }

  uint32_t a = h[0], b = h[1], c = h[2], d = h[3];
  uint32_t e = h[4], f = h[5], g = h[6], hh = h[7];

  for (int i = 0; i < 64; ++i) {
    const uint32_t temp1 = hh + bigSigma1(e) + choose(e, f, g) + K[i] + w[i];
    const uint32_t temp2 = bigSigma0(a) + majority(a, b, c);
    hh = g;
    g = f;
    f = e;
    e = d + temp1;
    d = c;
    c = b;
    b = a;
    a = temp1 + temp2;
  }

  h[0] += a;
  h[1] += b;
  h[2] += c;
  h[3] += d;
  h[4] += e;
  h[5] += f;
  h[6] += g;
  h[7] += hh;
}

}  // namespace

std::string sha256_hex(std::string_view data) {
  // FIPS 180-4 section 5.3.3: the initial hash value, the first 32 bits of the
  // fractional parts of the square roots of the first eight primes.
  std::array<uint32_t, 8> h = {0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};

  const uint64_t message_bits = static_cast<uint64_t>(data.size()) * 8;

  // Section 5.1.1 padding: append 0x80, then zero bytes until the length is 56 mod 64,
  // then the original bit length as a 64-bit big-endian integer. This always adds at
  // least one full extra block (9..72 padding bytes) so the length always lands on a
  // 64-byte boundary.
  size_t full_blocks = data.size() / 64;
  const uint8_t* base = reinterpret_cast<const uint8_t*>(data.data());
  for (size_t i = 0; i < full_blocks; ++i) {
    processBlock(h, base + i * 64);
  }

  const size_t remaining = data.size() - full_blocks * 64;
  uint8_t tail[128] = {};
  if (remaining > 0) {
    std::memcpy(tail, base + full_blocks * 64, remaining);
  }
  tail[remaining] = 0x80;
  const size_t tail_blocks = (remaining < 56) ? 1 : 2;
  const size_t length_offset = tail_blocks * 64 - 8;
  for (int i = 0; i < 8; ++i) {
    tail[length_offset + i] = static_cast<uint8_t>(message_bits >> (56 - 8 * i));
  }
  for (size_t i = 0; i < tail_blocks; ++i) {
    processBlock(h, tail + i * 64);
  }

  static const char* kHex = "0123456789abcdef";
  std::string out;
  out.reserve(64);
  for (uint32_t word : h) {
    for (int shift = 24; shift >= 0; shift -= 8) {
      const uint8_t byte = static_cast<uint8_t>(word >> shift);
      out.push_back(kHex[byte >> 4]);
      out.push_back(kHex[byte & 0x0f]);
    }
  }
  return out;
}

}  // namespace peadb

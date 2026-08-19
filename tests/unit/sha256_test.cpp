// FIPS 180-4 SHA-256 test vectors. This is the digest Redis's ACL subsystem uses to print
// a user's passwords as "#<hex>" in ACL LIST / ACL GETUSER (acl.c ACLHashPassword), so
// correctness here is a security requirement, not a nicety.

#include "sha256.hpp"

#include <iostream>
#include <string>

namespace {
int failures = 0;
void check(bool ok, const char* what) {
  if (!ok) { std::cerr << "FAIL: " << what << "\n"; ++failures; }
}
}  // namespace

int main() {
  // NIST / RFC 6234 published vectors (empty string, "abc", and the 56-byte two-block
  // message all lifted verbatim from FIPS 180-4 / RFC 6234 test data).
  check(peadb::sha256_hex("") ==
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        "empty string");
  check(peadb::sha256_hex("abc") ==
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "\"abc\"");
  check(peadb::sha256_hex("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq") ==
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1",
        "56-byte two-block message");

  // 448-bit padding boundary: a message of 55 bytes needs exactly one block (55 + 1 +
  // 8 == 64), while 56 bytes (checked above) spills into a second block. Expected value
  // computed independently: printf 'a%.0s' {1..55} | shasum -a 256.
  check(peadb::sha256_hex(std::string(55, 'a')) ==
            "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318",
        "55-byte one-block boundary");

  // One million 'a' is the classic long vector, exercising many-block processing.
  check(peadb::sha256_hex(std::string(1000000, 'a')) ==
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0",
        "one million 'a'");

  // Exact multiples of the 64-byte block size (one block, two blocks) exercise the
  // `remaining == 0` padding path, which none of the vectors above hit. Expected values
  // computed independently: python3 -c "import hashlib; print(hashlib.sha256(b'a'*64).hexdigest())".
  check(peadb::sha256_hex(std::string(64, 'a')) ==
            "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb",
        "64-byte exact one-block boundary");
  check(peadb::sha256_hex(std::string(128, 'a')) ==
            "6836cf13bac400e9105071cd6af47084dfacad4e5e302c94bfed24e013afb73e",
        "128-byte exact two-block boundary");

  if (failures == 0) std::cout << "sha256_test passed\n";
  return failures == 0 ? 0 : 1;
}

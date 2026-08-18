#pragma once

#include <string>
#include <string_view>

namespace peadb {

// FIPS 180-4 SHA-256, returned as 64 lowercase hex characters. Used for the ACL password
// digest, which Redis prints as "#<hex>" (acl.c ACLHashPassword / ACLDescribeUser).
std::string sha256_hex(std::string_view data);

}  // namespace peadb

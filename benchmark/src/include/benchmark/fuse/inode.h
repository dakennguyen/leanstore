#pragma once

#include "leanstore/leanstore.h"

namespace leanstore::fuse {

struct Inode {
  struct Key {
    uint64_t id;  // Unique inode identifier

    static uint64_t next_id;

    static auto Generate() -> Key { return Key{next_id++}; }
  };

  uint64_t size;      // Size of the file in bytes
  bool is_directory;  // True if this inode is a directory

  Inode() : size(0), is_directory(false) {}
  Inode(u_int64_t size, bool is_directory) : size(size), is_directory(is_directory) {}
};

uint64_t Inode::Key::next_id = 1;  // Initialize static member

}  // namespace leanstore::fuse

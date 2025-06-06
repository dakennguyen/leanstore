#pragma once

#include "leanstore/leanstore.h"
#include "benchmark/fuse/inode.h"

using FilePath = Varchar<128>;

namespace leanstore::fuse {

struct Dentry {
  struct Key {
    FilePath file_name;
  };

  leanstore::fuse::Inode::Key parent_inode_key;
  leanstore::fuse::Inode::Key inode_key;
};

}  // namespace leanstore::fuse

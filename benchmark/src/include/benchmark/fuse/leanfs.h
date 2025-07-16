#pragma once

#include "benchmark/fuse/schema.h"
#include "leanstore/leanstore.h"
#include "share_headers/db_types.h"

#include "benchmark/utils/rand.h"

#include "share_headers/logger.h"

#include <algorithm>
#include <array>
#include <variant>
#include <vector>

namespace leanstore::fuse {

template <template <typename> class AdapterType>
struct LeanFS {
  AdapterType<Inode> inodes;
  AdapterType<Dentry> dentries;
  AdapterType<FileRelation> files;

  inline static thread_local Integer inode_id_counter = 0;
  inline static thread_local Integer dentry_id_counter = 0;

  // Constructor
  template <typename... Params>
  LeanFS(Params &&...params)
      : inodes(AdapterType<Inode>(std::forward<Params>(params)...)),
        dentries(AdapterType<Dentry>(std::forward<Params>(params)...)),
        files(AdapterType<FileRelation>(std::forward<Params>(params)...)) {}

  // Methods
  auto AddInode(const Inode &record) -> Integer;
  auto RemoveDentry(Dentry::Key dentry_key) -> void;
  auto AddDentry(Varchar<256> file_name, Integer parent_dentry_id, const Dentry &record) -> Integer;

};

}  // namespace leanstore::fuse

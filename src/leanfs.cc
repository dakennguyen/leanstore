#include "benchmark/fuse/leanfs.h"
#include "benchmark/adapters/leanstore_adapter.h"

#include <algorithm>
#include <iostream>
#include <random>

namespace leanstore::fuse {

template <template <typename> class AdapterType>
auto LeanFS<AdapterType>::AddInode(const Inode &record) -> Integer {
  inodes.Insert({inode_id_counter}, record);
  return inode_id_counter++;
}

template <template <typename> class AdapterType>
auto LeanFS<AdapterType>::RemoveInode(Integer ino_id) -> void {
  // Remove the inode
  inodes.Erase({ino_id});

  // Remove relevant dentries
  std::vector<leanstore::fuse::Dentry::Key> dentry_keys;
  dentries.Scan({{}, {}}, [&](const auto &key, const auto &rec) {
    if (key.parent_inode_id == ino_id || rec.target_inode_id == ino_id) { dentry_keys.push_back(key); }
    return true;
  });
  for (const auto &dentry_key : dentry_keys) { dentries.Erase(dentry_key); }
}

template struct LeanFS<LeanStoreAdapter>;

}  // namespace leanstore::fuse

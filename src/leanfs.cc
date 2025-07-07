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
auto LeanFS<AdapterType>::RemoveDentry(Dentry::Key dentry_key) -> void {
  // Remove the dentry
  dentries.Erase(dentry_key);

  // Remove relevant dentries
  std::vector<leanstore::fuse::Dentry::Key> dentry_keys;
  dentries.Scan({{}, {}, dentry_key.id}, [&](const auto &key, const auto &rec) {
    if (key.parent_dentry_id == dentry_key.id) { dentry_keys.push_back(key); }
    return true;
  });
  for (const auto &dentry_key : dentry_keys) { dentries.Erase(dentry_key); }
}

template <template <typename> class AdapterType>
auto LeanFS<AdapterType>::AddDentry(Varchar<128> file_name, Integer parent_dentry_id, const Dentry &record) -> Integer {
  dentries.Insert({dentry_id_counter, file_name, parent_dentry_id}, record);
  return dentry_id_counter++;
}

template struct LeanFS<LeanStoreAdapter>;

}  // namespace leanstore::fuse

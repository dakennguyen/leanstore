#include "benchmark/fuse/leanfs.h"
#include "benchmark/adapters/leanstore_adapter.h"

#include <algorithm>
#include <random>
#include <iostream>

namespace leanstore::fuse {

template <template <typename> class AdapterType>
void LeanFS<AdapterType>::TestMethod(Integer i_id) {
  std::cout << i_id << std::endl;
}

template struct LeanFS<LeanStoreAdapter>;

}  // namespace leanstore::fuse

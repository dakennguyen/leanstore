#pragma once

#include "buffer/buffer_manager.h"
#include "storage/blob/blob_state.h"
#include "storage/blob/compressor.h"

#include "gtest/gtest_prod.h"
#include "roaring/roaring.hh"
#include "share_headers/db_types.h"

#include <functional>
#include <memory>
#include <span>
#include <unordered_set>

namespace leanstore::storage::blob {

using BlobCallbackFunc = std::function<void(std::span<const u8>)>;

// RAII Large Page Alias
struct PageAliasGuard {
  PageAliasGuard(buffer::BufferManager *buffer, const BlobState &blob, u64 required_load_size);
  ~PageAliasGuard();
  auto GetPtr() -> u8 *;

 private:
  u8 *ptr_{nullptr};
  buffer::BufferManager *buffer_;
};

class BlobManager {
 public:
  static thread_local BlobState *active_blob;
  static thread_local roaring::Roaring64Map extent_loaded;
  static thread_local std::array<u8, BlobState::MallocSize(ExtentList::EXTENT_CNT_MASK)> blob_handler_storage;

  explicit BlobManager(buffer::BufferManager *buffer_manager);
  ~BlobManager() = default;

  // Blob allocate/deallocate
  auto AllocateBlob(std::span<const u8> payload, const BlobState *prev_blob, bool likely_grow = true) -> BlobState *;
  void RemoveBlob(const BlobState *blob);

  // Blob Load/Unload utilities
  void LoadBlob(const BlobState *blob, u64 required_load_size, const BlobCallbackFunc &cb, off_t offset = 0);
  void UnloadAllBlobs();

  // Comparator utilities
  auto BlobStateCompareWithString(const void *a, const void *b) -> int;
  auto BlobStateComparison(const void *a, const void *b) -> int;

 private:
  void LoadBlobContent(const BlobState *blob, u64 required_load_size, off_t offset = 0);

  // Move data utilities
  auto WriteNewDataToLastExtent(transaction::Transaction &txn, std::span<const u8> payload, BlobState *blob) -> u64;
  auto MoveTailExtent(transaction::Transaction &txn, std::span<const u8> payload, BlobState *blob) -> u64;

  // Allocation utilities
  void MarkTailExtentForEviction(const TailExtent &special_blk, LargePageList &out_to_write_lps,
                                 LargePageList &out_to_evict_ets);
  void FreshBlobAllocation(std::span<const u8> payload, BlobState *out_blob, bool likely_grow,
                           LargePageList &out_to_write_lps, LargePageList &out_to_evict_ets);
  void ExtendExistingBlob(std::span<const u8> payload, BlobState *out_blob, LargePageList &out_to_write_lps,
                          LargePageList &out_to_evict_ets);

  buffer::BufferManager *buffer_;
};

}  // namespace leanstore::storage::blob
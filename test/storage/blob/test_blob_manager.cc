#include "storage/blob/blob_manager.h"
#include "storage/btree/tree.h"
#include "test/base_test.h"

#include "fmt/ranges.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <utility>

using leanstore::storage::blob::BlobManager;
using leanstore::storage::blob::BlobState;

namespace leanstore::buffer {

#define IS_EMPTY(blob_ptr, test_size)                                 \
  ({                                                                  \
    u8 tmp[BlobState::PageCount(test_size) * PAGE_SIZE];              \
    std::fill_n(tmp, BlobState::PageCount(test_size) * PAGE_SIZE, 0); \
    EXPECT_EQ(std::memcmp(blob_ptr, tmp, test_size), 0);              \
  })

class TestBlobManager : public BaseTest, public ::testing::WithParamInterface<std::tuple<int, bool, bool>> {
 protected:
  static constexpr u64 BLOB_SIZE = 18432;  // 4.5 * PAGE_SIZE,  3 Extents < Blob < 4 Extents
  u8 *random_blob_[2];
  std::unique_ptr<BlobManager> blob_manager_;

  void SetUp() override {
    BaseTest::SetupTestFile(true);
    for (auto idx = 0; idx < 2; idx++) {
      random_blob_[idx] = static_cast<u8 *>(malloc(BLOB_SIZE));
      for (size_t i = 0; i < BLOB_SIZE; i++) { random_blob_[idx][i] = (idx + 1) * 97 + i % 10; }
    }
    InitRandTransaction();
    blob_manager_ = std::make_unique<BlobManager>(buffer_.get());
  }

  void TearDown() override {
    for (auto idx = 0; idx < 2; idx++) { free(random_blob_[idx]); }
    buffer_->ReleaseAliasingArea();
    blob_manager_.reset();
    BaseTest::TearDown();
  }
};

TEST_P(TestBlobManager, InsertNewBlob) {
  FLAGS_blob_logging_variant    = std::get<0>(GetParam());
  auto blob_likely_grow         = std::get<1>(GetParam());
  FLAGS_blob_normal_buffer_pool = std::get<2>(GetParam());
  auto &txn                     = transaction::TransactionManager::active_txn;

  auto blob_payload    = std::span<u8>{random_blob_[0], BLOB_SIZE};
  auto blob_h          = blob_manager_->AllocateBlob(blob_payload, nullptr, blob_likely_grow);
  auto stored_blob_ptr = reinterpret_cast<u8 *>(buffer_->ToPtr(blob_h->BlobID()));
  if (blob_likely_grow) {
    ASSERT_EQ(blob_h->extents.extent_cnt, 3);
    ASSERT_FALSE(blob_h->extents.special_blk.in_used);
  } else {
    ASSERT_EQ(blob_h->extents.extent_cnt, 2);
    ASSERT_TRUE(blob_h->extents.special_blk.in_used);
  }
  EXPECT_EQ(blob_h->PageCount(), std::ceil(static_cast<float>(BLOB_SIZE) / PAGE_SIZE));
  EXPECT_EQ(std::memcmp(stored_blob_ptr, blob_payload.data(), BLOB_SIZE), 0);
  for (auto idx = 0; idx < blob_h->extents.NumberOfExtents(); idx++) {
    EXPECT_EQ(blob_h->extents.extent_pid[idx], txn.ToFlushedLargePages()[idx].start_pid);
  }

  // All states should be UNLOCKED - check `prevent_evict` description
  for (auto &extent : blob_h->extents) {
    CHECK_EXTENT_PAGE_STATE(sync::PageState::UNLOCKED, extent.start_pid, extent.page_cnt);
  }

  auto expected_phys_cnt = buffer_->physical_used_cnt_.load();
  {
    // Mock: write BLOB to disk
    auto ret = pwrite(test_file_fd_, buffer_->ToPtr(blob_h->BlobID()), blob_h->PageCount() * PAGE_SIZE,
                      blob_h->BlobID() * PAGE_SIZE);
    ASSERT_EQ(ret, static_cast<int>(blob_h->PageCount() * PAGE_SIZE));
  }

  // Evict all extents & special block if applicable
  u64 idx = 0;
  for (auto &extent : blob_h->extents) {
    EXPECT_EQ(extent, txn.ToEvictedExtents()[idx++]);
    buffer_->EvictExtent(extent.start_pid, extent.page_cnt);
  }
  ASSERT_EQ(txn.ToEvictedExtents().size(), 3);
  if (blob_likely_grow) {
    ASSERT_EQ(blob_h->extents.NumberOfExtents(), 3);  // 4.5 pages -> corresponds to three extents [1, 2, 4]
  } else {
    ASSERT_EQ(blob_h->extents.NumberOfExtents(), 2);  // 2 extents and the special block is used
    buffer_->EvictExtent(txn.ToEvictedExtents().back().start_pid, txn.ToEvictedExtents().back().page_cnt);
  }
  EXPECT_EQ(txn.ToEvictedExtents().size(), 3);
  ASSERT_EQ(blob_h->extents.NumberOfExtents(), 3 - static_cast<int>(!blob_likely_grow));
  if (FLAGS_blob_logging_variant == 0) {
    expected_phys_cnt -= storage::ExtentList::TotalSizeExtents(blob_h->extents.NumberOfExtents() - 1);
    if (!blob_likely_grow) { expected_phys_cnt -= blob_h->extents.special_blk.page_cnt; }
  } else {
    EXPECT_EQ(std::memcmp(stored_blob_ptr, random_blob_[0], BLOB_SIZE), 0);
  }
  EXPECT_EQ(buffer_->physical_used_cnt_.load(), expected_phys_cnt);

  u64 expected_state;
  switch (FLAGS_blob_logging_variant) {
    case 0: expected_state = sync::PageStateMode::EVICTED; break;
    case 1: expected_state = sync::PageStateMode::UNLOCKED; break;
    case 2: expected_state = sync::PageStateMode::MARKED; break;
    default: UnreachableCode(); break;
  }
  for (auto &extent : blob_h->extents) { CHECK_EXTENT_PAGE_STATE(expected_state, extent.start_pid, extent.page_cnt); }

  // Partially loaded -> only PageState[start_pid] is in SHARED state
  blob_manager_->LoadBlob(BlobManager::active_blob, PAGE_SIZE, [&](std::span<const u8> blob) {
    EXPECT_EQ(blob.size(), PAGE_SIZE);
    EXPECT_EQ(std::memcmp(stored_blob_ptr, random_blob_[0], PAGE_SIZE), 0);
  });
  EXPECT_FALSE(BlobManager::extent_loaded.isEmpty());

  auto first_extent = blob_h->extents[0];
  CHECK_EXTENT_PAGE_STATE(1, first_extent.start_pid, first_extent.page_cnt);
  auto second_extent = blob_h->extents[1];
  CHECK_EXTENT_PAGE_STATE(expected_state, second_extent.start_pid, second_extent.page_cnt);

  blob_manager_->LoadBlob(BlobManager::active_blob, BlobManager::active_blob->blob_size, [&](std::span<const u8> blob) {
    EXPECT_EQ(blob.size(), BLOB_SIZE);
    EXPECT_EQ(std::memcmp(stored_blob_ptr, random_blob_[0], BLOB_SIZE), 0);
  });

  for (auto &extent : blob_h->extents) { CHECK_EXTENT_PAGE_STATE(1, extent.start_pid, extent.page_cnt); }
  // TODO(Khoa): need to restore sha256.cc
  /*EXPECT_THAT(blob_h->sha2_val,*/
  /*            ::testing::ElementsAre(240, 55, 63, 44, 138, 135, 12, 194, 146, 233, 55, 136, 33, 194, 149, 128, 34, 237,*/
  /*                                   144, 228, 21, 199, 111, 124, 237, 167, 242, 34, 246, 249, 33, 250));*/
  blob_manager_->UnloadAllBlobs();
  for (auto &extent : blob_h->extents) {
    CHECK_EXTENT_PAGE_STATE(sync::PageStateMode::UNLOCKED, extent.start_pid, extent.page_cnt);
  }

  txn.ToFreeExtents().clear();
  // Now remove the Blob - there should be a single free range of 7 pages, start at `extents[0].start_pid`
  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 0);
  blob_manager_->RemoveBlob(blob_h);
  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 0);
  free_pages_->PublicFreeRanges(txn.ToFreeExtents());
  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 3);

  // Try acquire some free extents
  pageid_t start_pid;
  storage::TierList to_free_list;

  EXPECT_TRUE(REQUEST_TIER(0));
  EXPECT_EQ(start_pid, txn.ToEvictedExtents()[0].start_pid);
  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 2);
  EXPECT_TRUE(to_free_list.empty());

  // After request 1 free page, there should be 2 free ranges left of:
  //  - [2, 4] pages if blob_likely_grow == true
  //  - [2, 2] pages if blob_likely_grow == false
  if (blob_likely_grow) {
    EXPECT_FALSE(REQUEST_TIER(3));
    EXPECT_TRUE(REQUEST_TIER(2));
    EXPECT_EQ(start_pid, txn.ToEvictedExtents()[2].start_pid);
    EXPECT_FALSE(REQUEST_TIER(2));
    EXPECT_TRUE(REQUEST_TIER(1));
    EXPECT_EQ(start_pid, txn.ToEvictedExtents()[1].start_pid);
  } else {
    std::vector<pageid_t> temp;
    for (auto idx = 0; idx < 2; idx++) {
      EXPECT_FALSE(REQUEST_TIER(2));
      EXPECT_TRUE(REQUEST_TIER(1));
      temp.emplace_back(start_pid);
    }
    EXPECT_THAT(temp, ::testing::ElementsAre(txn.ToEvictedExtents()[1].start_pid, txn.ToEvictedExtents()[2].start_pid));
  }

  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 0);
}

TEST_P(TestBlobManager, GrowExistingBlob) {
  FLAGS_blob_logging_variant    = std::get<0>(GetParam());
  auto blob_likely_grow         = std::get<1>(GetParam());
  FLAGS_blob_normal_buffer_pool = std::get<2>(GetParam());
  auto &txn                     = transaction::TransactionManager::active_txn;

  // Allocate a blob first
  u8 root_blob_storage[BlobState::MAX_MALLOC_SIZE];
  auto blob_payload = std::span<u8>{random_blob_[0], BLOB_SIZE};
  auto blob_tmp     = blob_manager_->AllocateBlob(blob_payload, nullptr, blob_likely_grow);
  auto blob         = BlobState::MoveToTempStorage(root_blob_storage, blob_tmp);
  EXPECT_EQ(blob->PageCount(), std::ceil(static_cast<float>(BLOB_SIZE) / PAGE_SIZE));

  // Evict that blob to disk before appending new content to it
  auto expected_phys_cnt = buffer_->physical_used_cnt_.load();
  {
    // Mock: write BLOB to disk
    auto ret =
      pwrite(test_file_fd_, buffer_->ToPtr(blob->BlobID()), blob->PageCount() * PAGE_SIZE, blob->BlobID() * PAGE_SIZE);
    Ensure(ret == static_cast<int>(blob->PageCount() * PAGE_SIZE));
  }
  size_t idx = 0;
  for (auto &extent : blob->extents) {
    EXPECT_EQ(extent, txn.ToEvictedExtents()[idx++]);
    buffer_->EvictExtent(extent.start_pid, extent.page_cnt);
  }
  EXPECT_EQ(txn.ToEvictedExtents().size(), 3);
  if (blob_likely_grow) {
    ASSERT_EQ(blob->extents.extent_cnt, 3);
    ASSERT_FALSE(blob->extents.special_blk.in_used);
  } else {
    ASSERT_EQ(blob->extents.extent_cnt, 2);
    ASSERT_TRUE(blob->extents.special_blk.in_used);
    buffer_->EvictExtent(txn.ToEvictedExtents().back().start_pid, txn.ToEvictedExtents().back().page_cnt);
  }
  if (FLAGS_blob_logging_variant == 0) {
    expected_phys_cnt -= storage::ExtentList::TotalSizeExtents(blob->extents.NumberOfExtents() - 1);
    if (!blob_likely_grow) { expected_phys_cnt -= blob->extents.special_blk.page_cnt; }
  }
  EXPECT_EQ(buffer_->physical_used_cnt_.load(), expected_phys_cnt);

  // Allocate a random page before growing the Blob
  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 0);
  buffer_->AllocPage();
  expected_phys_cnt++;

  // Append to the old blob with 1 extra copy
  EXPECT_TRUE(BlobManager::extent_loaded.isEmpty());
  txn.ToFlushedLargePages().clear();
  txn.ToEvictedExtents().clear();
  auto grow_blob = blob_manager_->AllocateBlob(std::span<u8>{random_blob_[1], BLOB_SIZE}, blob);

  if (blob_likely_grow) {
    // Start txn log entry + PageImgEntry log entry
    auto exp_cursor = sizeof(recovery::LogEntry) + sizeof(recovery::PageImgEntry) + PAGE_SIZE / 2;
    EXPECT_EQ(transaction::TransactionManager::active_txn.LogWorker().w_cursor, exp_cursor);
    // Evaluate Log value
    auto log_offset = sizeof(recovery::LogEntry) + sizeof(recovery::PageImgEntry);
    auto log_entry  = &transaction::TransactionManager::active_txn.LogWorker().wal_buffer[log_offset];
    EXPECT_EQ(std::memcmp(log_entry, random_blob_[0], PAGE_SIZE / 2), 0);  // full of random_blob_[0][1]
  } else {
    // A whole new extent is allocated to store the content of the special block
    // Therefore, no log should be appended, i.e. only Start txn log entry if the log buffer
    EXPECT_EQ(transaction::TransactionManager::active_txn.LogWorker().w_cursor, sizeof(recovery::LogEntry));
  }

  // Appending existing blob requires loading all of its extents into the buffer manager
  // The last extent should be marked as Dirty and is waiting to be evicted & flushed to disk
  EXPECT_FALSE(BlobManager::extent_loaded.isEmpty());
  EXPECT_EQ(BlobManager::extent_loaded.cardinality(), 2);
  EXPECT_TRUE(BlobManager::extent_loaded.contains(blob->BlobID()));
  if (blob_likely_grow) {
    EXPECT_FALSE(blob->extents.special_blk.in_used);
  } else {
    EXPECT_TRUE(blob->extents.special_blk.in_used);
  }
  // The last extent is dirty, hence extent_loaded shouldnt contain that extent
  for (auto idx = 0UL; idx < BlobManager::extent_loaded.cardinality(); idx++) {
    EXPECT_TRUE(BlobManager::extent_loaded.contains(blob->extents.extent_pid[idx]));
  }

  // The last extent of `blob` has 3 remaining empty pages, which is not enough to store BLOB_SIZE (requires 4 pages)
  // Therefore, the expected flushed extents should be the prev last one + 1 new extent, which == 2
  EXPECT_EQ(txn.ToFlushedLargePages().size(), 2);
  if (blob_likely_grow) {
    EXPECT_EQ(txn.ToFlushedLargePages()[0].start_pid, txn.ToEvictedExtents()[0].start_pid + 1);
    EXPECT_EQ(txn.ToFlushedLargePages()[0].page_cnt, txn.ToEvictedExtents()[0].page_cnt - 1);
  } else {
    EXPECT_EQ(txn.ToFlushedLargePages()[0].start_pid, txn.ToEvictedExtents()[0].start_pid);
    EXPECT_EQ(txn.ToFlushedLargePages()[0].page_cnt, txn.ToEvictedExtents()[0].page_cnt);
  }
  EXPECT_EQ(txn.ToFlushedLargePages().size(), txn.ToEvictedExtents().size());
  for (idx = 0; idx < txn.ToEvictedExtents().size(); idx++) {
    EXPECT_EQ(grow_blob->extents[2 + idx], txn.ToEvictedExtents()[idx]);
  }

  // First two extents are in SHARED (1 owner), last two extents are UNLOCKED + prevent_evict==true
  EXPECT_EQ(grow_blob->extents.NumberOfExtents(), 4);
  for (auto idx = 0; idx < grow_blob->extents.NumberOfExtents(); idx++) {
    auto extent = grow_blob->extents[idx];
    if (idx < 2) {
      CHECK_EXTENT_PAGE_STATE(1, extent.start_pid, extent.page_cnt);
    } else {
      CHECK_EXTENT_PAGE_STATE(sync::PageState::UNLOCKED, extent.start_pid, extent.page_cnt);
    }
  }

  // Validate physical usage of buffer pool
  EXPECT_EQ(grow_blob->PageCount(), 2 * BLOB_SIZE / PAGE_SIZE);
  if (FLAGS_blob_logging_variant == 0) {
    // Prev content is evicted & loaded again. The special block should also be loaded if applicable
    expected_phys_cnt += storage::ExtentList::TotalSizeExtents(grow_blob->extents.NumberOfExtents() - 1);
    if (!blob_likely_grow) { expected_phys_cnt += blob->extents.special_blk.page_cnt; }
  } else {
    // Prev content is still in memory, no need to include it in the calculation
    expected_phys_cnt += storage::ExtentList::ExtentSize(grow_blob->extents.NumberOfExtents() - 1);
    // If there was a special block, then a new extent at
    //  index `grow_blob->extents.NumberOfExtents() - 2` will be allocated to store that block
    if (!blob_likely_grow) {
      expected_phys_cnt += storage::ExtentList::ExtentSize(grow_blob->extents.NumberOfExtents() - 2);
    }
  }
  EXPECT_EQ(buffer_->physical_used_cnt_.load(), expected_phys_cnt);

  // Validate the content of the grow blob
  {
    auto guard = storage::blob::PageAliasGuard(buffer_.get(), *grow_blob, grow_blob->blob_size);
    EXPECT_EQ(std::memcmp(guard.GetPtr(), random_blob_[0], BLOB_SIZE), 0);
    EXPECT_EQ(std::memcmp(guard.GetPtr() + BLOB_SIZE, random_blob_[1], BLOB_SIZE), 0);
  }
  // TODO(Khoa): need to restore sha256.cc
  /*EXPECT_THAT(grow_blob->sha2_val,*/
  /*            ::testing::ElementsAre(131, 150, 200, 204, 16, 190, 179, 154, 149, 104, 200, 138, 122, 19, 62, 59, 80,*/
  /*                                   140, 136, 103, 91, 233, 104, 20, 82, 232, 60, 96, 48, 203, 45, 160));*/

  // -------------------------------------------------------------------------------------
  {
    // Mock: write the grow BLOB to disk, no need to do it correctly according to dirty state
    for (auto &lp : txn.ToFlushedLargePages()) {
      auto ret = pwrite(test_file_fd_, buffer_->ToPtr(lp.start_pid), lp.page_cnt * PAGE_SIZE, lp.start_pid * PAGE_SIZE);
      Ensure(ret == static_cast<int>(lp.page_cnt * PAGE_SIZE));
    }
  }

  // Only the last two extents are dirty
  for (idx = grow_blob->extents.NumberOfExtents() - txn.ToEvictedExtents().size();
       idx < grow_blob->extents.NumberOfExtents(); idx++) {
    auto extent = grow_blob->extents[idx];
    buffer_->EvictExtent(extent.start_pid, extent.page_cnt);
  }
  if (FLAGS_blob_logging_variant == 0) {
    expected_phys_cnt -= storage::ExtentList::ExtentSize(grow_blob->extents.NumberOfExtents() - 1) +
                         storage::ExtentList::ExtentSize(grow_blob->extents.NumberOfExtents() - 2);
  }
  EXPECT_EQ(buffer_->physical_used_cnt_.load(), expected_phys_cnt);

  // Validate the state of all extents
  u64 expected_state;
  switch (FLAGS_blob_logging_variant) {
    case 0: expected_state = sync::PageStateMode::EVICTED; break;
    case 1: expected_state = sync::PageStateMode::UNLOCKED; break;
    case 2: expected_state = sync::PageStateMode::MARKED; break;
    default: UnreachableCode(); break;
  }
  for (idx = 0; idx < grow_blob->extents.NumberOfExtents() - txn.ToEvictedExtents().size(); idx++) {
    auto extent = grow_blob->extents[idx];
    CHECK_EXTENT_PAGE_STATE(1, extent.start_pid, extent.page_cnt);
  }
  for (idx = grow_blob->extents.NumberOfExtents() - txn.ToEvictedExtents().size();
       idx < grow_blob->extents.NumberOfExtents(); idx++) {
    auto extent = grow_blob->extents[idx];
    CHECK_EXTENT_PAGE_STATE(expected_state, extent.start_pid, extent.page_cnt);
  }

  // Partially loaded - only first page has data
  EXPECT_EQ(BlobManager::active_blob, grow_blob);
  blob_manager_->LoadBlob(BlobManager::active_blob, PAGE_SIZE, [&](std::span<const u8> blob_payload) {
    EXPECT_EQ(blob_payload.size(), PAGE_SIZE);
    EXPECT_EQ(std::memcmp(blob_payload.data(), random_blob_[0], PAGE_SIZE), 0);
  });
  EXPECT_FALSE(BlobManager::extent_loaded.isEmpty());
  EXPECT_EQ(BlobManager::extent_loaded.cardinality(), 2);

  // First two extents are in SHARED state, the rest are still in expected_state
  for (idx = 0; idx < grow_blob->extents.NumberOfExtents() - txn.ToEvictedExtents().size(); idx++) {
    auto extent = grow_blob->extents[idx];
    CHECK_EXTENT_PAGE_STATE(1, extent.start_pid, extent.page_cnt);
  }
  for (idx = grow_blob->extents.NumberOfExtents() - txn.ToEvictedExtents().size();
       idx < grow_blob->extents.NumberOfExtents(); idx++) {
    auto extent = grow_blob->extents[idx];
    CHECK_EXTENT_PAGE_STATE(expected_state, extent.start_pid, extent.page_cnt);
  }

  // Full load test
  blob_manager_->LoadBlob(
    BlobManager::active_blob, BlobManager::active_blob->blob_size, [&](std::span<const u8> blob_payload) {
      BlobState dump;
      dump.CalculateSHA256(blob_payload);
      // TODO(Khoa): need to restore sha256.cc
      /*EXPECT_THAT(dump.sha2_val,*/
      /*            ::testing::ElementsAre(131, 150, 200, 204, 16, 190, 179, 154, 149, 104, 200, 138, 122, 19, 62, 59, 80,*/
      /*                                   140, 136, 103, 91, 233, 104, 20, 82, 232, 60, 96, 48, 203, 45, 160));*/
      EXPECT_EQ(std::memcmp(blob_payload.data(), random_blob_[0], BLOB_SIZE), 0);
      EXPECT_EQ(std::memcmp(blob_payload.data() + BLOB_SIZE, random_blob_[1], BLOB_SIZE), 0);
    });
  for (auto &extent : grow_blob->extents) { CHECK_EXTENT_PAGE_STATE(1, extent.start_pid, extent.page_cnt); }

  blob_manager_->UnloadAllBlobs();
  for (auto &extent : grow_blob->extents) {
    CHECK_EXTENT_PAGE_STATE(sync::PageStateMode::UNLOCKED, extent.start_pid, extent.page_cnt);
  }

  // Now de-allocate this grow blob
  txn.ToFreeExtents().clear();
  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 0);
  blob_manager_->RemoveBlob(grow_blob);
  EXPECT_EQ(txn.ToFreeExtents().size(), 4);
  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 0);
  free_pages_->PublicFreeRanges(txn.ToFreeExtents());
  EXPECT_EQ(grow_blob->extents.extent_pid[3], 9 + ((blob_likely_grow) ? 0 : blob->extents.special_blk.page_cnt));
  // There should be a total of 4 allocated extents
  EXPECT_EQ(free_pages_->NumberOfFreeEntries(), 4);
  // There should be 4 ranges (format: [Start PID, size]): [1, 1], [2, 2], [4, 4], [8, 8]
  pageid_t start_pid;
  storage::TierList to_free_list;

  for (auto idx = 3; idx >= 0; --idx) {
    EXPECT_FALSE(REQUEST_TIER(idx + 1));
    EXPECT_TRUE(REQUEST_TIER(idx));
    EXPECT_EQ(start_pid, grow_blob->extents.extent_pid[idx]);
    EXPECT_TRUE(to_free_list.empty());
  }
}

TEST_P(TestBlobManager, InsertNewBlob2) {
  FLAGS_blob_logging_variant    = std::get<0>(GetParam());
  auto blob_likely_grow         = std::get<1>(GetParam());
  FLAGS_blob_normal_buffer_pool = std::get<2>(GetParam());

  auto blob_payload    = std::span<u8>{random_blob_[0], BLOB_SIZE};
  blob_manager_->AllocateBlob(blob_payload, nullptr, blob_likely_grow);

  // Load partially
  u8 *stored_blob_ptr1 = new u8[BLOB_SIZE];
  EXPECT_EQ(std::memcmp(stored_blob_ptr1, random_blob_[0], PAGE_SIZE), -97);
  blob_manager_->LoadBlob(BlobManager::active_blob, PAGE_SIZE, [&](std::span<const u8> blob) {
    EXPECT_EQ(blob.size(), PAGE_SIZE);
    std::memcpy(stored_blob_ptr1, blob.data(), blob.size());
  });
  EXPECT_EQ(std::memcmp(stored_blob_ptr1, random_blob_[0], PAGE_SIZE), 0);
  EXPECT_EQ(std::memcmp(stored_blob_ptr1, random_blob_[0], BLOB_SIZE), -103); // 97 + 4096 % 10
  delete[] stored_blob_ptr1;

  // Load fully
  u8 *stored_blob_ptr2 = new u8[BLOB_SIZE];
  blob_manager_->LoadBlob(BlobManager::active_blob, BlobManager::active_blob->blob_size, [&](std::span<const u8> blob) {
    EXPECT_EQ(blob.size(), BLOB_SIZE);
    std::memcpy(stored_blob_ptr2, blob.data(), blob.size());
  });
  EXPECT_EQ(std::memcmp(stored_blob_ptr2, random_blob_[0], BLOB_SIZE), 0);
  delete[] stored_blob_ptr2;

  // Load using offset
  off_t offset = 5183;
  u64 size = BLOB_SIZE - offset;
  u8 *stored_blob_ptr3 = new u8[BLOB_SIZE];
  blob_manager_->LoadBlob(BlobManager::active_blob, size, [&](std::span<const u8> blob) {
    EXPECT_EQ(blob.size(), size);
    std::memcpy(stored_blob_ptr3, blob.data(), blob.size());
  }, offset);
  EXPECT_EQ(std::memcmp(stored_blob_ptr3, random_blob_[0] + offset, size), 0);
  delete[] stored_blob_ptr3;
}

static constexpr auto TEST_SET{[]() constexpr {
  std::array<std::tuple<int, bool, bool>, 1> result{};
  auto idx = 0;
  for (int var : {1}) {
    for (bool likely_grow : {false}) {
      for (bool norm_bm : {false}) { result[idx++] = {var, likely_grow, norm_bm}; }
    }
  }
  return result;
}()};

INSTANTIATE_TEST_CASE_P(TestBlobManager, TestBlobManager, ::testing::ValuesIn(TEST_SET));

}  // namespace leanstore::buffer
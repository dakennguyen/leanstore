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
  off_t offset = 8191;
  u64 size = BLOB_SIZE - offset;
  u8 *stored_blob_ptr3 = new u8[BLOB_SIZE];
  blob_manager_->LoadBlob(BlobManager::active_blob, size, [&](std::span<const u8> blob) {
    EXPECT_EQ(blob.size(), size);
    std::memcpy(stored_blob_ptr3, blob.data() + 4095, blob.size());
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

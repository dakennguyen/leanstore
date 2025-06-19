#include "storage/blob/aliasing_guard.h"
#include "leanstore/leanstore.h"

namespace leanstore::storage::blob {

AliasingGuard::AliasingGuard(buffer::BufferManager *buffer, const BlobState &blob, u64 required_load_size, off_t offset)
    : buffer_(buffer) {
  // FLAGS_blob_normal_buffer_pool: 2nd extra overhead
  if (FLAGS_blob_normal_buffer_pool) {
    ptr_       = reinterpret_cast<u8 *>(malloc(required_load_size));
    u64 offset = 0;
    size_t idx = 0;
    for (; (idx < blob.extents.NumberOfExtents()) && (offset < required_load_size); idx++) {
      auto copy_size = std::min(required_load_size - offset, ExtentList::ExtentSize(idx) * PAGE_SIZE);
      buffer->ChunkOperation(blob.extents.extent_pid[idx], copy_size, [&](u64 off, std::span<u8> payload) {
        std::memcpy(&ptr_[offset + off], payload.data(), payload.size());
      });
      offset += copy_size;
    }
    if (blob.extents.tail_in_used && offset < required_load_size) {
      auto copy_size = std::min(required_load_size - offset, blob.extents.tail.page_cnt * PAGE_SIZE);
      buffer->ChunkOperation(blob.extents.tail.start_pid, copy_size, [&](u64 off, std::span<u8> payload) {
        std::memcpy(&ptr_[offset + off], payload.data(), payload.size());
      });
      offset += copy_size;
    }
    Ensure(offset >= required_load_size);
    return;
  }

  // Create Alias working area
  ptr_ = reinterpret_cast<u8 *>(buffer_->AliasArea()->RequestAliasingArea(blob.blob_size));

  // Prepare the aliasing params
  u64 alias_size = 0;
  size_t idx     = 0;
  size_t count   = 0;
  for (; (idx < blob.extents.NumberOfExtents()) && (alias_size < required_load_size); idx++) {
    auto extent      = blob.extents[idx];
    off_t start_byte = (extent.start_pid - 1) * PAGE_SIZE;
    off_t end_byte   = start_byte + extent.page_cnt * PAGE_SIZE - 1;
    if (offset > end_byte) { continue; }

    auto target_page_idx = offset < start_byte ? 0 : (offset - start_byte) / PAGE_SIZE;
    auto target_pid      = extent.start_pid + target_page_idx;
    auto target_page_cnt = std::min(extent.page_cnt - target_page_idx, required_load_size / PAGE_SIZE + 1);

    buffer->exmap_interface_[LeanStore::worker_thread_id]->iov[count].page = target_pid;
    buffer->exmap_interface_[LeanStore::worker_thread_id]->iov[count].len  = target_page_cnt;
    count++;
    alias_size += target_page_cnt * PAGE_SIZE;
  }
  if (blob.extents.tail_in_used && alias_size < required_load_size) {
    buffer->exmap_interface_[LeanStore::worker_thread_id]->iov[count].page = blob.extents.tail.start_pid;
    buffer->exmap_interface_[LeanStore::worker_thread_id]->iov[count].len  = blob.extents.tail.page_cnt;
    count++;
    alias_size += blob.extents.tail.page_cnt * PAGE_SIZE;
  }
  Ensure(alias_size >= required_load_size);

  // Aliasing the whole blob
  struct exmap_action_params params = {
    .interface = static_cast<u16>(LeanStore::worker_thread_id),
    .iov_len   = static_cast<u16>(count),
    .opcode    = static_cast<u16>(EXMAP_OP_SHADOW),
    .page_id   = buffer->ToPID(ptr_),
  };

  // Execute Exmap SHADOW operation
  Ensure(ioctl(buffer->exmapfd_, EXMAP_IOCTL_ACTION, &params) >= 0);

  // Point to the correct position within the first page
  ptr_ = ptr_ + (offset % PAGE_SIZE);
}

AliasingGuard::~AliasingGuard() {
  if (FLAGS_blob_normal_buffer_pool) {
    free(ptr_);
  } else {
    ExmapAction(buffer_->exmapfd_, EXMAP_OP_RM_SD, 0);
    buffer_->AliasArea()->ReleaseAliasingArea();
  }
}

auto AliasingGuard::GetPtr() -> u8 * { return ptr_; }

}  // namespace leanstore::storage::blob
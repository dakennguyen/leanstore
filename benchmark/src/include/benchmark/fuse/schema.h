#pragma once

#include "leanstore/leanstore.h"

#include "share_headers/db_types.h"
#include "typefold/typefold.h"

#include <cstring>

using FilePath = Varchar<256>;

namespace leanstore::fuse {

struct FileRelation {
  static constexpr int TYPE_ID = 0;

  struct Key {
    FilePath file_name;
  };

  leanstore::BlobState file_meta;

  // -------------------------------------------------------------------------------------
  auto PayloadSize() const -> uint32_t { return file_meta.MallocSize(); }

  static auto FoldKey(uint8_t *out, const Key &key) -> uint16_t {
    auto pos = Fold(out, key.file_name);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Key &key) -> uint16_t {
    auto pos = Unfold(in, key.file_name);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::file_name); }
};

struct Inode {
  struct Key {
    Integer id;
  };

  Numeric size;
  bool is_directory;

  auto PayloadSize() const -> uint32_t { return sizeof(Inode); }

  static auto FoldKey(uint8_t *out, const Inode::Key &key) -> uint16_t {
    auto pos = Fold(out, key.id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Inode::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::id); }
};

struct Dentry {
  struct Key {
    Integer id;
    Varchar<256> file_name;
    Integer parent_dentry_id;
  };

  Integer target_inode_id;

  auto PayloadSize() const -> uint32_t { return sizeof(Dentry); }

  static auto FoldKey(uint8_t *out, const Dentry::Key &key) -> uint16_t {
    auto pos = Fold(out, key.parent_dentry_id);
    pos += Fold(out + pos, key.file_name);
    pos += Fold(out + pos, key.id);
    return pos;
  }

  static auto UnfoldKey(const uint8_t *in, Dentry::Key &key) -> uint16_t {
    auto pos = Unfold(in, key.parent_dentry_id);
    pos += Unfold(in + pos, key.file_name);
    pos += Unfold(in + pos, key.id);
    return pos;
  }

  static auto MaxFoldLength() -> uint32_t { return 0 + sizeof(Key::parent_dentry_id) + sizeof(Key::file_name) + sizeof(Key::id); }
};

}  // namespace leanstore::fuse

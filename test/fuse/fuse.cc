#define FUSE_USE_VERSION 35

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/fuse/schema.h"
#include "leanstore/leanstore.h"

#include <fuse.h>
#include <cstring>

struct LeanStoreFUSE {
  static LeanStoreFUSE *obj;
  leanstore::LeanStore *db;
  std::unique_ptr<LeanStoreAdapter<leanstore::fuse::FileRelation>> adapter;

  LeanStoreFUSE(leanstore::LeanStore *db)
      : db(db), adapter(std::make_unique<LeanStoreAdapter<leanstore::fuse::FileRelation>>(*db)) {}

  ~LeanStoreFUSE() = default;

  static int GetAttr(const char *path, struct stat *stbuf) {
    std::string filename = path;
    int res              = 0;
    memset(stbuf, 0, sizeof(struct stat));

    stbuf->st_uid   = getuid();
    stbuf->st_gid   = getgid();
    stbuf->st_atime = stbuf->st_mtime = stbuf->st_ctime = time(NULL);

    if (filename == "/") {
      stbuf->st_mode  = S_IFDIR | 0777;
      stbuf->st_nlink = 2;
    } else {
      obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
        obj->db->StartTransaction();
        uint8_t blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
        uint64_t blob_rep_size = 0;

        auto file_path = FilePath(path);
        auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);

        auto found = obj->adapter->LookUp(file_key, [&](const auto &rec) {
          blob_rep_size = rec.PayloadSize();
          std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
        });
        if (!found) {
          res = -ENOENT;
          obj->db->CommitTransaction();
          return;
        }

        stbuf->st_mode  = S_IFREG | 0777;
        stbuf->st_nlink = 1;
        stbuf->st_size  = reinterpret_cast<leanstore::BlobState *>(blob_rep)->blob_size;
        obj->db->CommitTransaction();
      });
    }

    return res;
  }

  static int Open(const char *, struct fuse_file_info *) { return 0; }

  static int ReadDir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    filler(buf, ".", nullptr, 0);
    filler(buf, "..", nullptr, 0);
    filler(buf, "blob", nullptr, 0);
    filler(buf, "blob2", nullptr, 0);
    filler(buf, "hello", nullptr, 0);

    return 0;
  }

  static int Read(const char *path, char *buf, size_t size, off_t offset, [[maybe_unused]] struct fuse_file_info *fi) {
    int ret = 0;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();
      uint8_t blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
      uint64_t blob_rep_size = 0;

      auto file_path = FilePath(path);
      auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);

      auto found = obj->adapter->LookUp(file_key, [&](const auto &rec) {
        blob_rep_size = rec.PayloadSize();
        std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
      });
      if (!found) {
        ret = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }

      auto bh = reinterpret_cast<leanstore::BlobState *>(blob_rep);
      if (static_cast<u64>(offset) >= bh->blob_size) {
        ret = -EFAULT;
        obj->db->CommitTransaction();
        return;
      }

      obj->db->LoadBlob(
        bh, [&](std::span<const u8> content) { std::memcpy(buf, content.data() + offset, size); }, false);

      ret = std::min(size, bh->blob_size - offset);
      obj->db->CommitTransaction();
    });

    return ret;
  }

  static int Write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    int res = 0;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();

      // Look up
      uint8_t blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
      uint64_t blob_rep_size = 0;
      auto file_path = FilePath(path);
      auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);
      auto found = obj->adapter->LookUp(file_key, [&](const auto &rec) {
        blob_rep_size = rec.PayloadSize();
        std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
      });
      if (!found) {
        res = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }
      auto bh = reinterpret_cast<leanstore::BlobState *>(blob_rep);
      if (static_cast<u64>(offset) >= bh->blob_size) {
        res = -EFAULT;
        obj->db->CommitTransaction();
        return;
      }

      u8 payload[4096];
      obj->db->LoadBlob(
        bh, [&payload](std::span<const u8> content) { std::memcpy(payload, content.data(), content.size()); }, false);

      // Modify
      strcpy((char *)payload + offset, buf);
      auto blob_rep2 = obj->db->CreateNewBlob(payload, {}, false);

      // Update
      obj->adapter->UpdateRawPayload({path}, blob_rep2, [&](const auto &rec) {});

      obj->db->CommitTransaction();
    });

    return res;
  }

};

LeanStoreFUSE *LeanStoreFUSE::obj;

int main(int argc, char **argv) {
  // Initialize FUSE filesystem
  FLAGS_worker_count   = 1;
  FLAGS_bm_virtual_gb  = 128;
  FLAGS_bm_physical_gb = 32;
  FLAGS_db_path        = "/dev/nullb0";
  auto db              = std::make_unique<leanstore::LeanStore>();
  auto fs              = LeanStoreFUSE(db.get());
  LeanStoreFUSE::obj   = &fs;

  // Initialize temp BLOB
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    u8 payload[12288];
    for (auto idx = 0; idx < 12288; idx++) { payload[idx] = 97 + idx % 10; }
    auto blob_rep = db->CreateNewBlob({payload, 12288}, {}, false);
    fs.adapter->InsertRawPayload({"/blob"}, blob_rep);

    u8 payload2[4096];
    for (auto idx = 0; idx < 4096; idx++) { payload2[idx] = 124; }
    auto blob_rep2 = db->CreateNewBlob({payload2, 4096}, {}, false);
    fs.adapter->InsertRawPayload({"/blob2"}, blob_rep2);

    strcpy((char *)payload, "Hello World!");
    blob_rep = db->CreateNewBlob({payload, strlen((char *)payload)}, {}, false);
    fs.adapter->InsertRawPayload({"/hello"}, blob_rep);
    db->CommitTransaction();
  });

  struct fuse_operations fs_oper;
  fs_oper.open    = LeanStoreFUSE::Open;
  fs_oper.readdir = LeanStoreFUSE::ReadDir;
  fs_oper.read    = LeanStoreFUSE::Read;
  fs_oper.write   = LeanStoreFUSE::Write;
  fs_oper.getattr = LeanStoreFUSE::GetAttr;

  return fuse_main(argc, argv, &fs_oper, NULL);
}
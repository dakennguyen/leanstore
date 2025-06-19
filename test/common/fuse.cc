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

  explicit LeanStoreFUSE(leanstore::LeanStore *db)
      : db(db), adapter(std::make_unique<LeanStoreAdapter<leanstore::fuse::FileRelation>>(*db)) {}

  ~LeanStoreFUSE() = default;

  static auto GetAttr(const char *path, struct stat *stbuf) -> int {
    std::string filename = path;
    int res              = 0;
    memset(stbuf, 0, sizeof(struct stat));

    stbuf->st_uid   = getuid();
    stbuf->st_gid   = getgid();
    stbuf->st_atime = stbuf->st_mtime = stbuf->st_ctime = time(nullptr);

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

  static auto Open([[maybe_unused]] const char *path, [[maybe_unused]] struct fuse_file_info *inf) -> int { return 0; }

  static auto Read(const char *path, char *buf, size_t size, off_t offset, [[maybe_unused]] struct fuse_file_info *inf)
    -> int {
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

  static auto Create(const char *path, mode_t /*unused*/, struct fuse_file_info * /*unused*/) -> int {
    std::string str_path(path);
    size_t pos    = str_path.find_last_of('/');
    auto parent   = str_path.substr(0, pos);
    auto filename = str_path.substr(pos + 1);

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();
      obj->adapter->Insert({path}, {});
      obj->db->CommitTransaction();
    });

    return 0;
  }

  static auto Getxattr(const char * /*unused*/, const char * /*unused*/, char * /*unused*/, size_t /*unused*/) -> int {
    return 0;
  };

  static auto Access(const char * /*unused*/, int /*unused*/) -> int { return 0; }

  static auto Truncate(const char * /*unused*/, off_t /*unused*/) -> int { return 0; }

  static auto Utimens(const char * /*unused*/, const struct timespec /*unused*/[2]) -> int { return 0; }

  static auto Chown(const char * /*unused*/, uid_t /*unused*/, gid_t /*unused*/) -> int { return 0; }

  static auto Fsync(const char * /*unused*/, int /*unused*/, struct fuse_file_info * /*unused*/) -> int { return 0; };

  static auto Flush(const char * /*unused*/, struct fuse_file_info * /*unused*/) -> int { return 0; };

  // static auto MkDir(const char *path, mode_t /*unused*/) -> int {
  //   std::string str_path(path);
  //   size_t pos    = str_path.find_last_of('/');
  //   auto parent   = str_path.substr(0, pos);
  //   auto filename = str_path.substr(pos + 1);
  //
  //   int parent_inode_id = 0;
  //   int ino;
  //   obj->dblite->ui << "SELECT target_inode_id FROM dentry WHERE file_path = ?" << parent >>
  //     [&](int target_inode_id) { parent_inode_id = target_inode_id; };
  //   if (parent_inode_id == 0) { return -ENOENT; }
  //   obj->dblite->ui << "INSERT INTO inode (size, is_directory) VALUES (0, 1);";
  //   obj->dblite->ui << "SELECT last_insert_rowid();" >> [&](int inode_id) { ino = inode_id; };
  //
  //   obj->dblite->ui << fmt::format(
  //     "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('{}', '{}', {}, {});",
  //     str_path, filename, parent_inode_id, ino);
  //   obj->dblite->ui << fmt::format(
  //     "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('', '.', {}, {});", ino,
  //     ino);
  //   obj->dblite->ui << fmt::format(
  //     "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('', '..', {}, {});", ino,
  //     parent_inode_id);
  //
  //   return 0;
  // };

  static auto ReadDir([[maybe_unused]] const char *path, void *buf, fuse_fill_dir_t filler, off_t /*unused*/,
                      struct fuse_file_info * /*unused*/) -> int {
    filler(buf, ".", nullptr, 0);
    filler(buf, "..", nullptr, 0);
    filler(buf, "blob", nullptr, 0);
    filler(buf, "blob2", nullptr, 0);
    filler(buf, "hello", nullptr, 0);

    return 0;
  }

  static auto Unlink(const char *path) -> int {
    std::string filename = path;
    int ret = 0;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();
      uint8_t blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];

      auto file_path = FilePath(path);
      auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);

      auto found = obj->adapter->LookUp(file_key, [&](const auto &rec) {
        std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
      });
      if (!found) {
        ret = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }
      obj->adapter->RemoveBlob(blob_rep);
      obj->adapter->Erase(file_key);
      obj->db->CommitTransaction();
    });

    return ret;
  };

  static auto Write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info * /*unused*/)
    -> int {
    int res = 0;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();

      // Look up
      uint8_t blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
      uint64_t blob_rep_size = 0;
      auto file_path         = FilePath(path);
      auto file_key          = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);
      auto found             = obj->adapter->LookUp(file_key, [&](const auto &rec) {
        blob_rep_size = rec.PayloadSize();
        std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
      });
      if (!found) {
        res = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }
      auto bh = reinterpret_cast<leanstore::BlobState *>(blob_rep);

      std::span<const u8> span_bh2;
      if (bh->blob_size == 0 || (uint64_t)offset < bh->blob_size) {
        size_t payload_size = std::max(bh->blob_size, offset + size);
        u8 payload[payload_size];
        obj->db->LoadBlob(
          bh, [&payload](std::span<const u8> content) { std::memcpy(payload, content.data(), content.size()); });

        // Modify
        std::memcpy(payload + offset, buf, size);
        span_bh2 = obj->db->CreateNewBlob({payload, payload_size}, {}, false);
      } else {
        // Append
        u8 payload[size];
        std::memcpy(payload, buf, size);
        // auto start                            = std::chrono::high_resolution_clock::now();
        span_bh2 = obj->db->CreateNewBlob({payload, size}, bh, false);
        // auto end                              = std::chrono::high_resolution_clock::now();
        // std::chrono::duration<double> elapsed = end - start;
        // std::cout << "Elapsed time (total): " << elapsed.count() << " seconds" << std::endl;
      }

      // Update
      obj->adapter->UpdateRawPayload({path}, span_bh2, [&](const auto &) {});

      res = size;
      obj->db->CommitTransaction();
    });

    return res;
  }
};

LeanStoreFUSE *LeanStoreFUSE::obj;

static struct fuse_operations fs_oper = {
  .getattr = LeanStoreFUSE::GetAttr,
  .unlink   = LeanStoreFUSE::Unlink,
  .open    = LeanStoreFUSE::Open,
  .read    = LeanStoreFUSE::Read,
  .write    = LeanStoreFUSE::Write,
  .readdir = LeanStoreFUSE::ReadDir,
  .create  = LeanStoreFUSE::Create,
  // .chown    = LeanStoreFUSE::Chown,
  // .truncate = LeanStoreFUSE::Truncate,
  // .access  = LeanStoreFUSE::Access,
  .utimens  = LeanStoreFUSE::Utimens,
  // .fsync    = LeanStoreFUSE::Fsync,
  // .flush    = LeanStoreFUSE::Flush,
  // .getxattr = LeanStoreFUSE::Getxattr,
};

/**
 * @brief FUSE interface
 *
 * Command: `./test/LeanStoreFUSE -d -s -f /mnt/test`
 *
 * Read program:
 *
 * #include <fcntl.h>
 * #include <sys/stat.h>
 * #include <sys/syscall.h>
 * #include <cstdio>
 * #include <cstdlib>
 * #include <unistd.h>
 *
 * int main(){
 *     //./yfs1 is the mount point of my filesystem
 *     int fd = open("/mnt/test/hello", O_RDONLY);
 *     auto buf = (char *)malloc(4096);
 *     int readsize = read(fd, buf, 4096);
 *     printf("%d,%s,%d\n",fd, buf, readsize);
 *     close(fd);
 * }
 */
auto main(int argc, char **argv) -> int {
  // Initialize FUSE filesystem
  FLAGS_worker_count            = 1;
  FLAGS_bm_virtual_gb           = 128;
  FLAGS_bm_physical_gb          = 32;
  FLAGS_db_path                 = "/dev/nvme0n1";
  FLAGS_wal_stealing_group_size = 1;
  FLAGS_txn_commit_group_size   = 1;
  FLAGS_blob_enable             = true;
  auto db                       = std::make_unique<leanstore::LeanStore>();
  auto fs                       = LeanStoreFUSE(db.get());
  LeanStoreFUSE::obj            = &fs;

  // Initialize temp BLOB
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();

    u8 payload[12288];
    for (auto idx = 0; idx < 12288; idx++) { payload[idx] = 97 + idx % 10; }
    auto blob_rep = db->CreateNewBlob({payload, 12288}, {}, false);
    fs.adapter->InsertRawPayload({"/blob"}, blob_rep);

    u8 payload2[4096];
    for (unsigned char &byte : payload2) { byte = 124; }
    auto blob_rep2 = db->CreateNewBlob({payload2, 4096}, {}, false);
    fs.adapter->InsertRawPayload({"/blob2"}, blob_rep2);

    strcpy((char *)payload, "Hello World!");
    blob_rep = db->CreateNewBlob({payload, strlen((char *)payload)}, {}, false);
    fs.adapter->InsertRawPayload({"/hello"}, blob_rep);

    strcpy((char *)payload, "Temporary file in dir1");
    blob_rep = db->CreateNewBlob({payload, strlen((char *)payload)}, {}, false);
    fs.adapter->InsertRawPayload({"/dir1/tmp.txt"}, blob_rep);

    db->CommitTransaction();
  });

  return fuse_main(argc, argv, &fs_oper, nullptr);
}

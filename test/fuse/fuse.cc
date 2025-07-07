#include "benchmark/fuse/leanfs.h"
#include "fmt/ranges.h"
#include "share_headers/db_types.h"
#define FUSE_USE_VERSION 35

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/adapters/sql_databases.h"
#include "benchmark/fuse/schema.h"
#include "leanstore/leanstore.h"

#include <fuse3/fuse.h>
#include <cstring>
#include <iostream>
#include <string>

struct LeanStoreFUSE {
  static LeanStoreFUSE *obj;
  leanstore::LeanStore *db;
  std::unique_ptr<leanstore::fuse::LeanFS<LeanStoreAdapter>> leanfs;

  explicit LeanStoreFUSE(leanstore::LeanStore *db)
      : db(db), leanfs(std::make_unique<leanstore::fuse::LeanFS<LeanStoreAdapter>>(*db)) {}

  ~LeanStoreFUSE() = default;

 private:
  // This function need to be wrapped in a transaction
  static auto GetInodeFromPath(const std::string &str_path) -> int {
    auto path_str = str_path;
    if (path_str == "/") { return 0; }
    if (path_str.back() == '/') { path_str.pop_back(); }

    size_t pos            = path_str.find_last_of('/');
    auto parent           = path_str.substr(0, pos + 1);
    auto filename         = path_str.substr(pos + 1);
    auto filename_varchar = Varchar<128>(strdup(filename.c_str()));

    auto parent_inode_id = GetInodeFromPath(parent);
    int ino              = -1;
    obj->leanfs->dentries.LookUp({filename_varchar, parent_inode_id},
                                 [&](const auto &rec) { ino = rec.target_inode_id; });
    return ino;
  }

 public:
  static auto GetAttr(const char *path, struct stat *stbuf, struct fuse_file_info * /*unused*/) -> int {
    std::string str_path = path;

    int res = 0;
    memset(stbuf, 0, sizeof(struct stat));
    if (str_path == "/") {
      stbuf->st_mode  = S_IFDIR | 0777;
      stbuf->st_nlink = 2;
      return res;
    }

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();
      int ino = -1;
      ino     = GetInodeFromPath(str_path);
      if (ino == -1) {
        res = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }

      stbuf->st_uid   = getuid();
      stbuf->st_gid   = getgid();
      stbuf->st_atime = stbuf->st_mtime = stbuf->st_ctime = time(nullptr);

      bool is_directory = obj->leanfs->inodes.LookupField({ino}, &leanstore::fuse::Inode::is_directory);
      if (is_directory) {
        stbuf->st_mode  = S_IFDIR | 0777;
        stbuf->st_nlink = 2;
      } else {
        u8 blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
        u64 blob_rep_size = 0;

        auto file_path = FilePath(path);
        auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);

        auto found = obj->leanfs->files.LookUp(file_key, [&](const auto &rec) {
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
      }
      obj->db->CommitTransaction();
    });
    return res;
  }

  static auto Create(const char *path, mode_t /*unused*/, struct fuse_file_info * /*unused*/) -> int {
    int ret = 0;
    std::string str_path(path);
    size_t pos    = str_path.find_last_of('/');
    auto parent   = str_path.substr(0, pos + 1);
    auto filename = str_path.substr(pos + 1);

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();

      int parent_inode_id = -1;
      parent_inode_id     = GetInodeFromPath(parent);
      if (parent_inode_id == -1) {
        ret = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }

      int ino = obj->leanfs->AddInode({0, false});

      obj->leanfs->dentries.Insert({Varchar<128>(strdup(filename.c_str())), parent_inode_id}, {ino});
      obj->leanfs->files.Insert({path}, {});
      obj->db->CommitTransaction();
    });

    return ret;
  }

  static auto Open(const char * /*unused*/, struct fuse_file_info * /*unused*/) -> int { return 0; }

  static auto Getxattr(const char * /*unused*/, const char * /*unused*/, char * /*unused*/, size_t /*unused*/) -> int {
    return 0;
  };

  static auto Access(const char * /*unused*/, int /*unused*/) -> int { return 0; }

  static auto Truncate(const char * /*unused*/, off_t /*unused*/, struct fuse_file_info *fi) -> int { return 0; }

  static auto Utimens(const char * /*unused*/, const struct timespec tv[2], struct fuse_file_info *fi) -> int {
    return 0;
  }

  static auto Chown(const char * /*unused*/, uid_t /*unused*/, gid_t /*unused*/, struct fuse_file_info *fi) -> int {
    return 0;
  }

  static auto Fsync(const char * /*unused*/, int /*unused*/, struct fuse_file_info * /*unused*/) -> int { return 0; };

  static auto Flush(const char * /*unused*/, struct fuse_file_info * /*unused*/) -> int { return 0; };

  static auto MkDir(const char *path, mode_t /*unused*/) -> int {
    int ret = 0;
    std::string str_path(path);
    size_t pos    = str_path.find_last_of('/');
    auto parent   = str_path.substr(0, pos + 1);
    auto filename = str_path.substr(pos + 1);

    int parent_inode_id = -1;
    int ino;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();

      parent_inode_id = GetInodeFromPath(parent);
      if (parent_inode_id == -1) {
        ret = -ENOENT;
        return;
      }

      ino = obj->leanfs->AddInode({0, true});
      obj->leanfs->dentries.Insert({Varchar<128>(strdup(filename.c_str())), parent_inode_id}, {ino});
      obj->leanfs->dentries.Insert({Varchar<128>("."), ino}, {ino});
      obj->leanfs->dentries.Insert({Varchar<128>(".."), ino}, {parent_inode_id});

      obj->db->CommitTransaction();
    });

    return ret;
  };

  static auto ReadDir(const char *path, void *buf, fuse_fill_dir_t filler, off_t /*unused*/,
                      struct fuse_file_info * /*unused*/, enum fuse_readdir_flags /*unused*/) {
    int ret = 0;
    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();

      int parent_inode_id = -1;
      parent_inode_id     = GetInodeFromPath(path);
      if (parent_inode_id == -1) {
        ret = -ENOENT;
        return;
      }

      obj->leanfs->dentries.Scan({{}, parent_inode_id}, [&](const auto &key, const auto &) {
        if (key.parent_inode_id == parent_inode_id) {
          filler(buf, key.file_name.CStr(), nullptr, 0, static_cast<fuse_fill_dir_flags>(0));
          return true;
        }
        return false;
      });

      obj->db->CommitTransaction();
    });

    return 0;
  }

  static auto Unlink(const char *path) -> int {
    int ret = 0;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();

      int ino = -1;
      ino     = GetInodeFromPath(path);
      if (ino == -1) {
        ret = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }

      obj->leanfs->RemoveInode(ino);

      // Remove blob
      u8 blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];

      auto file_path = FilePath(path);
      auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);

      auto found = obj->leanfs->files.LookUp(file_key, [&](const auto &rec) {
        std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
      });
      if (!found) {
        ret = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }
      obj->leanfs->files.RemoveBlob(blob_rep);
      obj->leanfs->files.Erase(file_key);
      obj->db->CommitTransaction();
    });

    return ret;
  };

  static auto Read(const char *path, char *buf, size_t size, off_t offset, [[maybe_unused]] struct fuse_file_info *fi)
    -> int {
    int ret = 0;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();
      u8 blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
      u64 blob_rep_size = 0;

      auto file_path = FilePath(path);
      auto file_key  = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);

      auto found = obj->leanfs->files.LookUp(file_key, [&](const auto &rec) {
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

      obj->leanfs->files.LoadBlob(
        blob_rep, [&](std::span<const u8> content) { std::memcpy(buf, content.data(), content.size()); }, size, offset);

      ret = std::min(size, bh->blob_size - offset);
      obj->db->CommitTransaction();
    });

    return ret;
  }

  static auto Write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info * /*unused*/)
    -> int {
    // std::cout << path << " " << buf << " " << size << " " << offset << std::endl;
    int res = 0;

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();

      // Look up
      u8 blob_rep[leanstore::BlobState::MAX_MALLOC_SIZE];
      u64 blob_rep_size = 0;
      auto file_path    = FilePath(path);
      auto file_key     = reinterpret_cast<leanstore::fuse::FileRelation::Key &>(file_path);
      auto found        = obj->leanfs->files.LookUp(file_key, [&](const auto &rec) {
        blob_rep_size = rec.PayloadSize();
        std::memcpy(blob_rep, const_cast<leanstore::fuse::FileRelation &>(rec).file_meta.Data(), rec.PayloadSize());
      });
      if (!found) {
        res = -ENOENT;
        obj->db->CommitTransaction();
        return;
      }
      auto bh = reinterpret_cast<leanstore::BlobState *>(blob_rep);

      std::span<const u8> updated_bh;
      if (bh->blob_size == 0) {
        // if (offset > 0) {
        //   res = -EFAULT;
        //   obj->db->CommitTransaction();
        //   return;
        // }
        // New blob
        u8 payload[size];
        std::memcpy(payload, buf, size);
        updated_bh = obj->leanfs->files.RegisterBlob({payload, size}, {}, false);
      } else if ((u64)offset < bh->blob_size) {
        // Not supported for now
        res = -EPERM;
        obj->db->CommitTransaction();
        return;

        size_t payload_size = std::max(bh->blob_size, offset + size);
        auto payload        = std::make_unique<u8[]>(payload_size);

        // Load the whole blob
        obj->leanfs->files.LoadBlob(
          blob_rep,
          [&payload](std::span<const u8> content) { std::memcpy(payload.get(), content.data(), content.size()); },
          bh->blob_size, 0);

        // Replace everything
        std::memcpy(payload.get() + offset, buf, size);
        std::span<u8> payload_span(payload.get(), payload_size);
        updated_bh = obj->leanfs->files.RegisterBlob(payload_span, {}, false);
      } else {
        // Append
        u8 payload[size];
        std::memcpy(payload, buf, size);
        updated_bh = obj->leanfs->files.RegisterBlob({payload, size}, blob_rep, true);
      }

      // Update
      obj->leanfs->files.UpdateRawPayload({path}, updated_bh, [&](const auto &) {});

      res = size;
      obj->db->CommitTransaction();
    });

    return res;
  }

  static auto Init(struct fuse_conn_info *conn, struct fuse_config *cfg) -> void * {
    conn->max_readahead = 1024 * 1024;
    cfg->direct_io      = 1;

    return nullptr;
  }
};

LeanStoreFUSE *LeanStoreFUSE::obj;

auto main(int argc, char **argv) -> int {
  // Initialize FUSE filesystem
  FLAGS_exmap_path     = "/dev/exmap0";
  FLAGS_worker_count   = 1;
  FLAGS_bm_virtual_gb  = 128;
  FLAGS_bm_physical_gb = 32;
  FLAGS_db_path        = "/dev/nvme0n1";
  auto db              = std::make_unique<leanstore::LeanStore>();
  auto fs              = LeanStoreFUSE(db.get());
  LeanStoreFUSE::obj   = &fs;

  // Initialize temp BLOB
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();

    auto root_inode_id = fs.leanfs->AddInode({0, true});
    fs.leanfs->dentries.Insert({".", root_inode_id}, {root_inode_id});
    fs.leanfs->dentries.Insert({"..", root_inode_id}, {root_inode_id});

    auto inserted_id = fs.leanfs->AddInode({0, false});
    fs.leanfs->dentries.Insert({"blob", root_inode_id}, {inserted_id});
    u8 payload[12288];
    for (auto idx = 0; idx < 12288; idx++) { payload[idx] = 97 + idx % 10; }
    auto blob_rep = db->CreateNewBlob({payload, 12288}, {}, false);
    fs.leanfs->files.InsertRawPayload({"/blob"}, blob_rep);

    inserted_id = fs.leanfs->AddInode({0, false});
    fs.leanfs->dentries.Insert({"blob2", root_inode_id}, {inserted_id});
    u8 payload2[4096];
    for (unsigned char &byte : payload2) { byte = 124; }
    auto blob_rep2 = db->CreateNewBlob({payload2, 4096}, {}, false);
    fs.leanfs->files.InsertRawPayload({"/blob2"}, blob_rep2);

    inserted_id = fs.leanfs->AddInode({0, false});
    fs.leanfs->dentries.Insert({"hello", root_inode_id}, {inserted_id});
    strcpy((char *)payload, "Hello World!");
    blob_rep = db->CreateNewBlob({payload, strlen((char *)payload)}, {}, false);
    fs.leanfs->files.InsertRawPayload({"/hello"}, blob_rep);

    int dir1_ino = fs.leanfs->AddInode({0, true});
    fs.leanfs->dentries.Insert({"dir1", root_inode_id}, {dir1_ino});
    fs.leanfs->dentries.Insert({".", dir1_ino}, {dir1_ino});
    fs.leanfs->dentries.Insert({"..", dir1_ino}, {root_inode_id});

    inserted_id = fs.leanfs->AddInode({0, false});
    fs.leanfs->dentries.Insert({"tmp.txt", dir1_ino}, {inserted_id});
    strcpy((char *)payload, "Temporary file in dir1");
    blob_rep = db->CreateNewBlob({payload, strlen((char *)payload)}, {}, false);
    fs.leanfs->files.InsertRawPayload({"/dir1/tmp.txt"}, blob_rep);

    db->CommitTransaction();
  });

  static struct fuse_operations fs_oper;
  fs_oper.init     = LeanStoreFUSE::Init;
  fs_oper.getattr  = LeanStoreFUSE::GetAttr;
  fs_oper.mkdir    = LeanStoreFUSE::MkDir;
  fs_oper.unlink   = LeanStoreFUSE::Unlink;
  fs_oper.chown    = LeanStoreFUSE::Chown;
  fs_oper.truncate = LeanStoreFUSE::Truncate;
  fs_oper.open     = LeanStoreFUSE::Open;
  fs_oper.read     = LeanStoreFUSE::Read;
  fs_oper.write    = LeanStoreFUSE::Write;
  fs_oper.flush    = LeanStoreFUSE::Flush;
  fs_oper.fsync    = LeanStoreFUSE::Fsync;
  fs_oper.getxattr = LeanStoreFUSE::Getxattr;
  fs_oper.readdir  = LeanStoreFUSE::ReadDir;
  fs_oper.access   = LeanStoreFUSE::Access;
  fs_oper.create   = LeanStoreFUSE::Create;
  fs_oper.utimens  = LeanStoreFUSE::Utimens;

  return fuse_main(argc, argv, &fs_oper, nullptr);
}

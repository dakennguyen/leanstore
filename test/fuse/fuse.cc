#include "fmt/ranges.h"
#define FUSE_USE_VERSION 35
#include <iostream>

#include "benchmark/adapters/leanstore_adapter.h"
#include "benchmark/adapters/sql_databases.h"
#include "benchmark/fuse/schema.h"
#include "leanstore/leanstore.h"

#include <fuse.h>
#include <cstring>
#include <string>

struct LeanStoreFUSE {
  static LeanStoreFUSE *obj;
  leanstore::LeanStore *db;
  std::unique_ptr<LeanStoreAdapter<leanstore::fuse::FileRelation>> adapter;
  std::unique_ptr<SQLiteDB> dblite;

  explicit LeanStoreFUSE(leanstore::LeanStore *db)
      : db(db),
        adapter(std::make_unique<LeanStoreAdapter<leanstore::fuse::FileRelation>>(*db)),
        dblite(std::make_unique<SQLiteDB>("/home/khoa/projects/leanstore/fs.sqlite")) {}

  ~LeanStoreFUSE() = default;

  static int GetAttr(const char *path, struct stat *stbuf) {
    std::string filename = path;
    int ino              = 0;
    obj->dblite->ui << "SELECT target_inode_id, file_name FROM dentry WHERE file_path = ?" << filename >>
      [&](int target_inode_id, const std::string &file_name) {
        if (file_name != "..") { ino = target_inode_id; }
      };
    if (ino == 0) { return -ENOENT; }

    int res = 0;
    memset(stbuf, 0, sizeof(struct stat));

    stbuf->st_uid   = getuid();
    stbuf->st_gid   = getgid();
    stbuf->st_atime = stbuf->st_mtime = stbuf->st_ctime = time(nullptr);

    bool is_directory = false;
    obj->dblite->ui << "SELECT is_directory FROM inode WHERE id = ?" << ino >>
      [&](bool is_dir) { is_directory = is_dir; };

    if (is_directory) {
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

  static int Create(const char *path, mode_t /*unused*/, struct fuse_file_info * /*unused*/) {
    std::string str_path(path);
    size_t pos    = str_path.find_last_of('/');
    auto parent   = str_path.substr(0, pos);
    auto filename = str_path.substr(pos + 1);

    int parent_inode_id = 0;
    int ino;
    obj->dblite->ui << "SELECT target_inode_id FROM dentry WHERE file_path = ?" << parent >>
      [&](int target_inode_id) { parent_inode_id = target_inode_id; };
    if (parent_inode_id == 0) { return -ENOENT; }

    obj->dblite->ui << "INSERT INTO inode (size, is_directory) VALUES (0, 0);";
    obj->dblite->ui << "SELECT last_insert_rowid();" >> [&](int inode_id) { ino = inode_id; };
    obj->dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('{}', '{}', {}, {});",
      str_path, filename, parent_inode_id, ino);

    obj->db->worker_pool.ScheduleSyncJob(0, [&]() {
      obj->db->StartTransaction();
      obj->adapter->Insert({path}, {});
      obj->db->CommitTransaction();
    });

    return 0;
  }

  static int Open(const char * /*unused*/, struct fuse_file_info * /*unused*/) { return 0; }

  static int Getxattr(const char * /*unused*/, const char * /*unused*/, char * /*unused*/, size_t /*unused*/) {
    return 0;
  };

  static int Access(const char * /*unused*/, int /*unused*/) { return 0; }

  static int Truncate(const char * /*unused*/, off_t /*unused*/) { return 0; }

  static int Utimens(const char * /*unused*/, const struct timespec /*unused*/[2]) { return 0; }

  static int Chown(const char * /*unused*/, uid_t /*unused*/, gid_t /*unused*/) { return 0; }

  static int Fsync(const char * /*unused*/, int /*unused*/, struct fuse_file_info * /*unused*/) { return 0; };

  static int Flush(const char * /*unused*/, struct fuse_file_info * /*unused*/) { return 0; };

  static int MkDir(const char *path, mode_t /*unused*/) {
    std::string str_path(path);
    size_t pos    = str_path.find_last_of('/');
    auto parent   = str_path.substr(0, pos);
    auto filename = str_path.substr(pos + 1);

    int parent_inode_id = 0;
    int ino;
    obj->dblite->ui << "SELECT target_inode_id FROM dentry WHERE file_path = ?" << parent >>
      [&](int target_inode_id) { parent_inode_id = target_inode_id; };
    if (parent_inode_id == 0) { return -ENOENT; }
    obj->dblite->ui << "INSERT INTO inode (size, is_directory) VALUES (0, 1);";
    obj->dblite->ui << "SELECT last_insert_rowid();" >> [&](int inode_id) { ino = inode_id; };

    obj->dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('{}', '{}', {}, {});",
      str_path, filename, parent_inode_id, ino);
    obj->dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('', '.', {}, {});", ino,
      ino);
    obj->dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('', '..', {}, {});", ino,
      parent_inode_id);

    return 0;
  };

  static int ReadDir(const char *path, void *buf, fuse_fill_dir_t filler, off_t /*unused*/,
                     struct fuse_file_info * /*unused*/) {
    int parent_inode_id = 0;
    obj->dblite->ui << "SELECT target_inode_id FROM dentry WHERE file_path = ?" << path >>
      [&](int target_inode_id) { parent_inode_id = target_inode_id; };
    if (parent_inode_id == 0) { return -ENOENT; }

    obj->dblite->ui
        << "SELECT file_name FROM dentry JOIN inode ON dentry.target_inode_id = inode.id WHERE parent_inode_id = ?;"
        << parent_inode_id >>
      [&](const std::string &file_name) { filler(buf, strdup(file_name.c_str()), nullptr, 0); };

    return 0;
  }

  static int Unlink(const char *path) {
    std::string filename = path;
    int ino              = 0;
    obj->dblite->ui << "SELECT target_inode_id, file_name FROM dentry WHERE file_path = ?" << filename >>
      [&](int target_inode_id, const std::string &file_name) {
        if (file_name != "..") { ino = target_inode_id; }
      };
    if (ino == 0) { return -ENOENT; }
    obj->dblite->ui << "PRAGMA foreign_keys = ON;";
    obj->dblite->ui << "DELETE FROM inode WHERE id = ?;" << ino;

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

      // for (int i = 0; i < 32; i++) {
      //   std::cout << "DEBUG sha2_val[" << i << "]: " << (int)bh->sha2_val[i] << std::endl;
      // }

      obj->db->LoadBlob(
        bh, [&](std::span<const u8> content) { std::memcpy(buf, content.data(), content.size()); }, size, offset);

      ret = std::min(size, bh->blob_size - offset);
      obj->db->CommitTransaction();
    });

    return ret;
  }

  static int Write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info * /*unused*/) {
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
          bh, [&payload](std::span<const u8> content) { std::memcpy(payload, content.data(), content.size()); }, 0);

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

int main(int argc, char **argv) {
  // Initialize FUSE filesystem
  FLAGS_exmap_path     = "/dev/exmap0";
  FLAGS_worker_count   = 1;
  FLAGS_bm_virtual_gb  = 128;
  FLAGS_bm_physical_gb = 32;
  FLAGS_db_path        = "/dev/nvme0n1";
  auto db              = std::make_unique<leanstore::LeanStore>();
  auto fs              = LeanStoreFUSE(db.get());
  LeanStoreFUSE::obj   = &fs;

  fs.dblite->StartTransaction();
  fs.dblite->ui << "DROP TABLE IF EXISTS INODE;";
  fs.dblite->ui << "DROP TABLE IF EXISTS DENTRY;";
  fs.dblite->ui << "CREATE TABLE inode ( id INTEGER PRIMARY KEY, size INTEGER NOT NULL, is_directory BOOLEAN NOT NULL "
                   "CHECK (is_directory IN (0, 1)));";
  fs.dblite->ui << "CREATE TABLE dentry ( file_path TEXT NOT NULL, file_name TEXT NOT NULL, parent_inode_id INTEGER "
                   "NOT NULL, target_inode_id INTEGER NOT NULL, FOREIGN KEY (parent_inode_id) REFERENCES inode(id) ON "
                   "DELETE CASCADE, "
                   "FOREIGN KEY (target_inode_id) REFERENCES inode(id) ON DELETE CASCADE, PRIMARY KEY (file_name, "
                   "parent_inode_id));";
  fs.dblite->CommitTransaction();

  // Initialize temp BLOB
  db->worker_pool.ScheduleSyncJob(0, [&]() {
    db->StartTransaction();
    fs.dblite->StartTransaction();

    auto root_inode_id = 1;
    fs.dblite->ui << fmt::format("INSERT INTO inode (id, size, is_directory) VALUES ({}, 0, 1);", root_inode_id);
    fs.dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('/', '.', {}, {});",
      root_inode_id, root_inode_id);
    fs.dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('', '..', {}, {});",
      root_inode_id, root_inode_id);

    fs.dblite->ui << "INSERT INTO inode (id, size, is_directory) VALUES (2, 0, 0);";
    fs.dblite->ui
      << "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('/blob', 'blob', 1, 2);";
    u8 payload[12288];
    for (auto idx = 0; idx < 12288; idx++) { payload[idx] = 97 + idx % 10; }
    auto blob_rep = db->CreateNewBlob({payload, 12288}, {}, false);
    fs.adapter->InsertRawPayload({"/blob"}, blob_rep);

    fs.dblite->ui << "INSERT INTO inode (id, size, is_directory) VALUES (3, 0, 0);";
    fs.dblite->ui << "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('/blob2', "
                     "'blob2', 1, 3);";
    u8 payload2[4096];
    for (unsigned char &byte : payload2) { byte = 124; }
    auto blob_rep2 = db->CreateNewBlob({payload2, 4096}, {}, false);
    fs.adapter->InsertRawPayload({"/blob2"}, blob_rep2);

    fs.dblite->ui << "INSERT INTO inode (id, size, is_directory) VALUES (4, 0, 0);";
    fs.dblite->ui << "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('/hello', "
                     "'hello', 1, 4);";
    strcpy((char *)payload, "Hello World!");
    blob_rep = db->CreateNewBlob({payload, strlen((char *)payload)}, {}, false);
    fs.adapter->InsertRawPayload({"/hello"}, blob_rep);

    fs.dblite->ui << fmt::format("INSERT INTO inode (id, size, is_directory) VALUES ({}, 0, 1);", 5);
    fs.dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('/dir1', 'dir1', {}, {});",
      1, 5);
    fs.dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('', '.', {}, {});", 5, 5);
    fs.dblite->ui << fmt::format(
      "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES ('', '..', {}, {});", 5, 1);

    fs.dblite->ui << "INSERT INTO inode (id, size, is_directory) VALUES (6, 0, 0);";
    fs.dblite->ui << "INSERT INTO dentry (file_path, file_name, parent_inode_id, target_inode_id) VALUES "
                     "('/dir1/tmp.txt', 'tmp.txt', 5, 6);";
    strcpy((char *)payload, "Temporary file in dir1");
    blob_rep = db->CreateNewBlob({payload, strlen((char *)payload)}, {}, false);
    fs.adapter->InsertRawPayload({"/dir1/tmp.txt"}, blob_rep);

    fs.dblite->CommitTransaction();
    db->CommitTransaction();
  });

  static struct fuse_operations fs_oper;
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

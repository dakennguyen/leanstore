#pragma once
#include "Units.hpp"
// -------------------------------------------------------------------------------------
#include "gflags/gflags.h"
// -------------------------------------------------------------------------------------
DECLARE_uint32(dram); // 1 GiB
DECLARE_uint32(ssd); // 10 GiB
DECLARE_string(ssd_path);
DECLARE_string(free_pages_list_path);
DECLARE_uint32(cool);
DECLARE_uint32(free);
DECLARE_uint32(async_batch_size);
DECLARE_uint32(falloc);
DECLARE_bool(trunc);
// -------------------------------------------------------------------------------------
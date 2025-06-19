# LeanStore

[LeanStore](https://db.in.tum.de/~leis/papers/leanstore.pdf) is a high-performance OLTP storage engine optimized for many-core CPUs and NVMe SSDs. Our goal is to achieve performance comparable to in-memory systems when the data set fits into RAM, while being able to fully exploit the bandwidth of fast NVMe SSDs for large data sets. While LeanStore is currently a research prototype, we hope to make it usable in production in the future.

## Compiling
Install dependencies:

### Core

`sudo apt-get install autoconf automake libtool curl make cmake g++ unzip libtbb-dev libfmt-dev libgflags-dev libgtest-dev libgmock-dev liburing-dev libzstd-dev libcurl4-openssl-dev libbenchmark-dev`

**exmap**: stored in `share_libs/exmap`
- Run `sudo ./load.sh`

### Third-party libraries

**Databases**: `sudo apt-get install libwiredtiger-dev libsqlite3-dev libmysqlcppconn-dev libpq-dev libfuse-dev librocksdb-dev`

## How to build

`mkdir build && cd build && cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && make -j`

## Troubleshoot

Install g++ 13:

    wget https://gcc.gnu.org/pub/gcc/releases/gcc-13.3.0/gcc-13.3.0.tar.gz
    tar -xvzf gcc-13.3.0.tar.gz
    cd gcc-13.3.0
    ./contrib/download_prerequisites
    ./configure --disable-multilib --enable-languages=c,c++
    make -j3
    sudo make install
    sudo update-alternatives --install /usr/bin/gcc gcc /usr/local/bin/gcc 60 --slave /usr/bin/g++ g++ /usr/local/bin/g++

Missing GLIBCXX_3.4.31:

    export LD_LIBRARY_PATH=/usr/local/lib64

    # To confirm
    find /usr/local -name libstdc++.so*
    strings /usr/local/lib64/libstdc++.so.6 | grep GLIBCXX_3.4.31


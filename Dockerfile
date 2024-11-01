FROM ubuntu:24.04

################################################################################
# install prerequisted libriaries
################################################################################
RUN apt-get update && apt-get install -y \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        build-essential g++-aarch64-linux-gnu gcc-aarch64-linux-gnu \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        git cmake make gcc g++ ninja-build \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        libaio-dev python3-pip cppcheck \
 && rm -rf /var/lib/apt/lists/*


# install vcpkg and its prerequisites
RUN apt-get update && apt-get install -y \
        curl zip unzip tar pkg-config \
 && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y \
        autoconf libtool golang graphviz sysstat \
        libtbb-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev librocksdb-dev liblmdb-dev libwiredtiger-dev liburing-dev \
 && rm -rf /var/lib/apt/lists/*


################################################################################
# other settings
################################################################################
USER root
WORKDIR /root
CMD ["/usr/bin/bash"]

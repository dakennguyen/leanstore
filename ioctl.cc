#include <fcntl.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <cstring>
#include <iostream>

#define FUSE_IOCTL_SET_SORT_STR _IOW('f', 1, char[128])

int main() {
    const char* path = "/mnt/test4";
    int fd = open(path, O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    std::string sort_str = "name:desc";  // Could also be "mtime:asc", etc.
    char buf[128] = {};
    std::strncpy(buf, sort_str.c_str(), sizeof(buf) - 1);

    if (ioctl(fd, FUSE_IOCTL_SET_SORT_STR, buf) < 0) {
        perror("ioctl");
        return 1;
    }

    close(fd);
    return 0;
}

# LeanStore as FUSE

**Command**: `./test/LeanStoreFUSE -d -s -f /mnt/test`

**Sample read**
```C++
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <cstdio>
#include <cstdlib>
#include <unistd.h>

int main(){
    //./yfs1 is the mount point of my filesystem
    int fd = open("/mnt/test/hello", O_RDONLY);
    auto buf = (char *)malloc(4096);
    int readsize = read(fd, buf, 4096);
    printf("%d,%s,%d\n",fd, buf, readsize);
    close(fd);
}
```

**Sample write**
```C++
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>

int main(int argc, char *argv[]) {
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0] << " <file> <position> <replace string>\n";
    return 1;
  }
  const std::string filename = argv[1];

  // Open the file in binary mode for reading and writing
  std::fstream file(filename, std::ios::in | std::ios::out | std::ios::binary);
  if (!file) {
    std::cerr << "Failed to open file: " << filename << std::endl;
    return 1;
  }

  // Position to the byte offset where the change is needed (e.g., 10th byte)
  std::streampos position = std::stoll(argv[2]);

  // Seek to the position
  file.seekp(position);
  if (!file) {
    std::cerr << "Seek failed!" << std::endl;
    return 1;
  }

  // Write the new data
  char *data = argv[3];
  file.write(data, strlen(data));
  if (!file) {
    std::cerr << "Write failed!" << std::endl;
    return 1;
  }

  file.close();
  std::cout << "Data successfully modified!" << std::endl;

  return 0;
}
```

# IOMgr

## Build
To build (assuming a local Docker instance is running):
```
   $ docker build -t iomgr-pub --build-arg CONAN_USER=$(whoami) --build-arg CONAN_PASS=<secret> -f Dockerfile .
```
To publish:
```
   $ docker run --rm iomgr-pub
```

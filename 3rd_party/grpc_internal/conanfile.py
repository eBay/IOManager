from conans import ConanFile, tools
import os

required_conan_version = ">=1.43.0"

class grpcInternalConan(ConanFile):
    name = "grpc_internal"
    description = "Google's RPC (remote procedure call) library and framework."
    topics = ("grpc", "rpc")
    url = "https://github.com/conan-io/conan-center-index"
    homepage = "https://github.com/grpc/grpc"
    license = "Apache-2.0"
    version = "1.48.0"

    @property
    def _source_subfolder(self):
        return "grpc_internal"

    def source(self):
        tools.get("https://github.com/grpc/grpc/archive/v1.48.0.tar.gz",
                  destination=self._source_subfolder, strip_root=True)

    def package(self):
        self.copy("*.h", dst="include/", src="./", keep_path=True)

    def package_info(self):
        self.cpp_info.includedirs = ["include/", "include/grpc_internal"]

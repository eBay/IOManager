#https://github.com/SPDK/spdk"!/usr/bin/env python
# -*- coding: utf-8 -*-

from conans import ConanFile, AutoToolsBuildEnvironment, tools
from conan.tools.files import patch, copy, get

class LibSPDKConan(ConanFile):
    name = "spdk"
    version = "21.07.y"
    description = "Data Plane Development Kit"
    url = "https://github.corp.ebay.com/conan/spdk"
    homepage = "https://github.com/SPDK/spdk"
    license = "BSD-3"
    exports = ["LICENSE.md", "install.diff"]
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "native_build": [True, False],
        "shared": [True, False],
        "fPIC": [True, False]
    }
    default_options = (
        "native_build=False",
        "shared=False",
        "fPIC=True",
    )

    requires = (
        "dpdk/21.05",
        "liburing/2.1",
        "openssl/1.1.1q",
        )
    build_requires = (
        "fio/3.28"
        )

    exports_sources = "patches/*"

    def configure(self):
        del self.settings.compiler.libcxx

    def source(self):
        get(self, "https://github.com/SPDK/spdk/archive/96a91684d39bb0f18729f132a24b778876d3e8fc.tar.gz", strip_root=True)

    def build(self):
        patch(self, patch_file="patches/patch.diff", strip=1)
        autotools = AutoToolsBuildEnvironment(self)
        autotools.flags.append("-I{}/include".format(self.deps_cpp_info["openssl"].rootpath))
        cargs = ["--with-dpdk={}".format(self.deps_cpp_info["dpdk"].rootpath),
                "--without-vhost",
                "--without-virtio",
                "--with-fio={}/include/fio".format(self.deps_cpp_info["fio"].rootpath),
                "--with-uring={}/include".format(self.deps_cpp_info["liburing"].rootpath),
                "--without-isal",
                "--disable-tests",
                "--disable-unit-tests"]
        if not self.options.native_build:
            cargs.append("--target-arch=corei7")
            tools.replace_in_file("configure", "x86_64", "corei7")
            tools.replace_in_file("configure", "march=native", "march=corei7")
        if self.settings.build_type == "Debug":
            cargs.append("--enable-debug")
        autotools.configure(args=cargs)
        autotools.make(vars={
                                "LD_TYPE":"bfd",
                                "LDFLAGS": "-L{0}/lib -L{1}/lib".format(self.deps_cpp_info["openssl"].rootpath,
                                                                        self.deps_cpp_info["liburing"].rootpath)
                            })

    def package(self):
        self.copy("common.sh", dst="scripts", src="scripts", keep_path=True)
        self.copy("setup.sh", dst="scripts", src="scripts", keep_path=True)
        self.copy("spdkcli*", dst="scripts", src="scripts", keep_path=True)
        self.copy("rpc*", dst="scripts", src="scripts", keep_path=True)
        self.copy("*.h", dst="include/spdk/lib", src="lib", keep_path=True)
        self.copy("*.h", dst="include/spdk/module", src="module", keep_path=True)
        autotools = AutoToolsBuildEnvironment(self)
        autotools.install()

    def deploy(self):
        self.copy("*", dst="/usr/local/bin/spdk", src="bin")
        self.copy("*", dst="/var/lib/spdk/scripts", src="scripts")
        self.copy("*pci_ids.h", dst="/var/lib/spdk/include", src="include")

    def package_info(self):
        self.cpp_info.libs = [
                              "-Wl,--whole-archive -lspdk_accel_ioat",
                              "spdk_blobfs",
                              "spdk_blob_bdev",
                              "spdk_bdev_uring",
                              "spdk_bdev_malloc",
                              "spdk_bdev_error",
                              "spdk_bdev_passthru",
                              "spdk_bdev_split",
                              "spdk_bdev_aio",
                              "spdk_bdev_nvme",
                              "spdk_bdev_gpt",
                              "spdk_bdev_raid",
                              "spdk_bdev_null",
                              "spdk_event_sock",
                              "spdk_event_vmd",
                              "spdk_event_bdev",
                              "spdk_event_accel",
                              "spdk_event_scsi",
                              "spdk_event",
                              "spdk_init",
                              "spdk_bdev",
                              "spdk_ioat",
                              "spdk_rpc",
                              "spdk_scsi",
                              "spdk_sock_uring",
                              "spdk_sock_posix",
                              "spdk_sock",
                              "spdk_accel",
                              "spdk_vmd",
                              "spdk_ftl",
                              "spdk_conf",
                              "spdk_blob",
                              "spdk_nvme",
                              "spdk_thread",
                              "spdk_log",
                              "spdk_trace",
                              "spdk_notify",
                              "spdk_env_dpdk",
                              "spdk_util",
                              "spdk_jsonrpc",
                              "-lspdk_json -Wl,--no-whole-archive",
                              "aio", "rt", "pthread", "uuid", "m"]
        self.env_info.RTE_SDK = self.package_folder

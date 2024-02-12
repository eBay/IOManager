from conan import ConanFile
from conan.tools.gnu import Autotools, AutotoolsToolchain, AutotoolsDeps
from conan.tools.files import apply_conandata_patches
from conan.tools.files import patch, get, replace_in_file
from conan.tools.env import Environment, VirtualBuildEnv

required_conan_version = ">=1.60.0"

class LibSPDKConan(ConanFile):
    name = "spdk"
    description = "Data Plane Development Kit"
    url = "https://github.corp.ebay.com/conan/spdk"
    homepage = "https://github.com/SPDK/spdk"
    license = "BSD-3"
    exports = ["LICENSE.md", "install.diff"]
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "native_build": ['True', 'False'],
        "shared": ['True', 'False'],
        "fPIC": ['True', 'False']
    }
    default_options = {
        "native_build":False,
        "shared":False,
        "fPIC":True,
    }

    requires = (
        )
    build_requires = (
        )

    exports_sources = "patches/*"

    def requirements(self):
        self.requires("dpdk/21.05", transitive_headers=True)
        self.requires("liburing/2.1", transitive_headers=True)
        self.requires("openssl/[>=1.1 <4]")
        self.requires("fio/3.28")

    def configure(self):
        del self.settings.compiler.libcxx

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
        apply_conandata_patches(self)

    def generate(self):
        tc = AutotoolsToolchain(self)
        e = tc.environment()
        e.append("CFLAGS", "-I{}/include".format(self.dependencies['openssl'].package_folder))
        e.append("CFLAGS", "-I{}/include".format(self.dependencies['liburing'].package_folder))
        e.append("LDFLAGS", "-L{}/lib".format(self.dependencies['openssl'].package_folder))
        e.append("LDFLAGS", "-L{}/lib".format(self.dependencies['liburing'].package_folder))
        e.append("LD_TYPE", "bfd")

        #autotools.flags.append("-I{}/include".format(self.deps_cpp_info["openssl"].rootpath))
        tc.configure_args = ["--with-dpdk={}".format(self.dependencies['dpdk'].package_folder),
                "--without-vhost",
                "--without-virtio",
                "--with-fio={}/include/fio".format(self.dependencies['fio'].package_folder),
                "--with-uring={}/include".format(self.dependencies['liburing'].package_folder),
                "--without-isal",
                "--disable-tests",
                "--disable-unit-tests"]
        if not self.options.native_build:
            tc.configure_args.append("--target-arch=corei7")
            replace_in_file(self, "configure", "x86_64", "corei7")
            replace_in_file(self, "configure", "march=native", "march=corei7")
        if self.settings.build_type == "Debug":
            tc.configure_args.append("--enable-debug")
        tc.generate(e)

        td = AutotoolsDeps(self)
        td.generate()

    def build(self):
        autotools = Autotools(self)
        autotools.configure()
        autotools.make()

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

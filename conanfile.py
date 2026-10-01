from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake
from conan.tools.files import copy
from os.path import join

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "8.8.7"
    homepage = "https://github.com/eBay/IOManager"
    description = "Asynchronous event manager"
    topics = ("ebay", "nublox", "aio")
    url = "https://github.com/eBay/IOManager"
    license = "Apache-2.0"

    settings = "arch", "os", "compiler", "build_type"

    options = {
        "shared": ['True', 'False'],
        "fPIC": ['True', 'False'],
        "coverage": ['True', 'False'],
        "sanitize": ['True', 'False'],
        "testing" : ['full', 'off', 'epoll_mode', 'spdk_mode'],
        }
    default_options = {
        'shared':       False,
        'fPIC':         True,
        'coverage':     False,
        'sanitize':     False,
        'testing':      'epoll_mode',
        'sisl:prerelease':   True,
        "pistache:with_ssl": True,
    }

    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*", "LICENSE"

    def configure(self):
        if self.options.shared:
            del self.options.fPIC
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
        if self.settings.arch != "x86_64":
            self.options["spdk"].native_build = True

    def build_requirements(self):
        self.test_requires("gtest/1.15.0")

    def requirements(self):
        self.requires("sisl/8.9.8")

        self.requires("boost/1.79.0")
        self.requires("folly/2022.01.31.00")
        self.requires("grpc/1.54.3")
        self.requires("grpc_internal/1.48.0")
        self.requires("liburing/2.1")
        self.requires("nlohmann_json/[^3.11]")
        self.requires("libevent/2.1.12")
        self.requires("spdk/nbi.21.07.y")
        self.requires("pistache/nbi.0.0.5.1")

        self.requires("openssl/1.1.1s", override=True)

    def layout(self):
        self.folders.source = "."
        if self.options.get_safe("sanitize"):
            self.folders.build = join("build", "Sanitized")
        elif self.options.get_safe("coverage"):
            self.folders.build = join("build", "Coverage")
        else:
            self.folders.build = join("build", str(self.settings.build_type))
        self.folders.generators = join(self.folders.build, "generators")

        self.cpp.source.includedirs = ["src/include"]

        self.cpp.build.libdirs = ["src/lib"]
        self.cpp.build.includedirs = ["src/include"]

        self.cpp.package.libs = ["iomgr"]
        self.cpp.package.includedirs = ["include"] # includedirs is already set to 'include' by
        self.cpp.package.libdirs = ["lib"]

    def generate(self):
        # This generates "conan_toolchain.cmake" in self.generators_folder
        tc = CMakeToolchain(self)
        tc.variables['CMAKE_TEST_TARGET'] = self.options.testing
        tc.variables['CMAKE_EXPORT_COMPILE_COMMANDS'] = 'ON'
        tc.variables["CTEST_OUTPUT_ON_FAILURE"] = "ON"
        tc.variables["MEMORY_SANITIZER_ON"] = "OFF"
        tc.variables["BUILD_COVERAGE"] = "OFF"
        if self.settings.build_type == "Debug":
            if self.options.get_safe("coverage"):
                tc.variables['BUILD_COVERAGE'] = 'ON'
            elif self.options.get_safe("sanitize"):
                tc.variables['MEMORY_SANITIZER_ON'] = 'ON'
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        if not self.conf.get("tools.build:skip_test", default=False):
            cmake.test()

    def package(self):
        copy(self, "LICENSE", self.source_folder, join(self.package_folder, "licenses"), keep_path=False)
        copy(self, "*.h", join(self.source_folder, "src", "include"), join(self.package_folder, "include", "iomgr"), keep_path=True)
        copy(self, "*.hpp", join(self.source_folder, "src", "include"), join(self.package_folder, "include", "iomgr"), keep_path=True)
        copy(self, "*iomgr_config_generated.h", join(self.build_folder, "src"), join(self.package_folder, "include", "iomgr"), keep_path=False)
        copy(self, "*.a", self.build_folder, join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.so", self.build_folder, join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dylib", self.build_folder, join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dll", self.build_folder, join(self.package_folder, "lib"), keep_path=False)

    def package_info(self):
        self.cpp_info.libs = ["iomgr"]
        self.cpp_info.cxxflags.append("-fconcepts")
        if self.settings.build_type == "Debug":
            if  self.options.sanitize:
                self.cpp_info.sharedlinkflags.append("-fsanitize=address")
                self.cpp_info.exelinkflags.append("-fsanitize=address")
                self.cpp_info.sharedlinkflags.append("-fsanitize=undefined")
                self.cpp_info.exelinkflags.append("-fsanitize=undefined")
            elif self.options.coverage == 'True':
                self.cpp_info.libs.append('gcov')

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake
from conan.tools.files import copy
from os.path import join, exists

required_conan_version = ">=1.60.0"

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "12.1.1"

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
        "grpc_support": ['True', 'False'],
        "spdk": ['True', 'False'],
        }
    default_options = {
        'shared':       False,
        'fPIC':         True,
        'coverage':     False,
        'grpc_support': False,
        'sanitize':     False,
        'testing':      'epoll_mode',
        'spdk':         True,
    }

    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*", "LICENSE"

    def _min_cppstd(self):
        return 20

    def validate(self):
        if self.settings.compiler.get_safe("cppstd"):
            check_min_cppstd(self, self._min_cppstd())

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
            if self.conf.get("tools.build:skip_test", default=False):
                if self.options.coverage or self.options.sanitize:
                    raise ConanInvalidConfiguration("Coverage/Sanitizer requires Testing!")
        if self.settings.arch != "x86_64":
            self.options["spdk"].native_build = True

    def build_requirements(self):
        self.test_requires("gtest/1.14.0")
        self.test_requires("cpr/1.10.4")

    def requirements(self):
        self.requires("sisl/[^13.1]@oss/master", transitive_headers=True)
        if self.options.grpc_support:
            self.requires("grpc/[>=1.50]")
        if self.options.spdk:
            self.requires("spdk/nbi.21.07.y", transitive_headers=True)
        self.requires("pistache/nbi.0.0.5.1", transitive_headers=True)
        self.requires("libcurl/8.4.0", override=True)
        self.requires("lz4/1.9.4", override=True)
        self.requires("zstd/1.5.5", override=True)

        # ARM needs unreleased versionof libunwind
        if not self.settings.arch in ['x86', 'x86_64']:
            self.requires("libunwind/1.8.2@baydb/develop", override=True)

    def _download_grpc(self, folder):
        ref = self.dependencies['grpc'].ref.version
        source_info = self.dependencies['grpc'].conan_data["sources"][f"{ref}"]
        current_info_str = json.dumps(source_info, sort_keys=True)

        touch_file_path = join(folder, "grpc_download")

        if exists(touch_file_path) == False or load(self, touch_file_path) != current_info_str:
            print("-------------- downloading grpc sources ---------")
            get(self, **source_info, destination=join(folder, "grpc_internal"), strip_root=True)
            save(self, touch_file_path, current_info_str)

    def layout(self):
        if self.options.grpc_support:
            self._download_grpc(self.source_folder)

        self.folders.source = "."
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
        tc.variables["CONAN_CMAKE_SILENT_OUTPUT"] = "ON"
        tc.variables['CMAKE_EXPORT_COMPILE_COMMANDS'] = 'ON'
        tc.variables["CTEST_OUTPUT_ON_FAILURE"] = "ON"
        tc.variables["MEMORY_SANITIZER_ON"] = "OFF"
        tc.variables["BUILD_COVERAGE"] = "OFF"
        tc.variables["CMAKE_TEST_TARGET"] = self.options.testing
        if self.settings.build_type == "Debug":
            if self.options.get_safe("coverage"):
                tc.variables['BUILD_COVERAGE'] = 'ON'
            elif self.options.get_safe("sanitize"):
                tc.variables['MEMORY_SANITIZER_ON'] = 'ON'
        tc.generate()

        # This generates "boost-config.cmake" and "grpc-config.cmake" etc in self.generators_folder
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
        copy(self, "*.h", join(self.source_folder, "src", "include"), join(self.package_folder, "include"), keep_path=True)
        copy(self, "*.hpp", join(self.source_folder, "src", "include"), join(self.package_folder, "include"), keep_path=True)
        copy(self, "*iomgr_config_generated.h", join(self.build_folder, "src"), join(self.package_folder, "include", "iomgr"), keep_path=False)
        copy(self, "*.a", self.build_folder, join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.so", self.build_folder, join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dylib", self.build_folder, join(self.package_folder, "lib"), keep_path=False)
        copy(self, "*.dll", self.build_folder, join(self.package_folder, "lib"), keep_path=False)

    def package_info(self):
        if  self.options.sanitize:
            self.cpp_info.sharedlinkflags.append("-fsanitize=address")
            self.cpp_info.exelinkflags.append("-fsanitize=address")
            self.cpp_info.sharedlinkflags.append("-fsanitize=undefined")
            self.cpp_info.exelinkflags.append("-fsanitize=undefined")
        elif self.options.coverage == 'True':
            self.cpp_info.libs.append('gcov')

from conan import ConanFile
from conan.errors import ConanInvalidConfiguration
from conan.tools.build import check_min_cppstd
from conan.tools.cmake import CMakeToolchain, CMakeDeps, CMake
from conan.tools.files import copy
from os.path import join, exists

required_conan_version = ">=1.60.0"

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "13.0.0"

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
        "sanitize": ['address', 'thread', 'False'],
        'prerelease' : ['True', 'False'],
        "testing" : ['full', 'off', 'epoll_mode'],
        }
    default_options = {
        'shared':       False,
        'fPIC':         True,
        'coverage':     False,
        'sanitize':     False,
        'prerelease':   False,
        'testing':      'epoll_mode',
    }

    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*", "LICENSE"

    def _min_cppstd(self):
        return 23

    def validate(self):
        if self.settings.compiler.get_safe("cppstd"):
            check_min_cppstd(self, self._min_cppstd())

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")
        if self.settings.build_type == "Debug":
            self.options.rm_safe("prerelease")
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
            if self.conf.get("tools.build:skip_test", default=False):
                if self.options.coverage or self.options.sanitize:
                    raise ConanInvalidConfiguration("Coverage/Sanitizer requires Testing!")
    def build_requirements(self):
        self.test_requires("gtest/[^1.17]")
        self.test_requires("cpr/[^1.12]")

    def requirements(self):
        self.requires("sisl/[^14.4]@oss/dev", transitive_headers=True)
        if self.settings.os in ["Linux"]:
            self.requires("liburing/[^2.1]", transitive_headers=True)

    def layout(self):
        self.folders.source = "."
        if self.options.get_safe("sanitize") and self.options.sanitize != "False":
            self.folders.build = join("build", f"Sanitized-{self.options.sanitize}")
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
        tc.variables["CONAN_CMAKE_SILENT_OUTPUT"] = "ON"
        tc.variables['CMAKE_EXPORT_COMPILE_COMMANDS'] = 'ON'
        tc.variables["CTEST_OUTPUT_ON_FAILURE"] = "ON"
        tc.variables["MEMORY_SANITIZER_ON"] = "OFF"
        tc.variables["BUILD_COVERAGE"] = "OFF"
        tc.variables["PRERELEASE_ON"] = "OFF"
        tc.variables["CMAKE_TEST_TARGET"] = self.options.testing
        if self.options.get_safe("prerelease"):
            tc.variables["PRERELEASE_ON"] = "ON"
        if self.settings.build_type == "Debug":
            if self.options.get_safe("coverage"):
                tc.variables['BUILD_COVERAGE'] = 'ON'
            elif self.options.get_safe("sanitize") and self.options.sanitize != "False":
                if self.options.sanitize == "thread":
                    tc.variables['THREAD_SANITIZER_ON'] = 'ON'
                else:  # address
                    tc.variables['ADDRESS_SANITIZER_ON'] = 'ON'
        tc.variables["CONAN_PACKAGE_NAME"] = self.name
        tc.variables["CONAN_PACKAGE_VERSION"] = self.version
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
        if not self.conf.get("tools.build:skip_test", default=False):
            self.run(f"ctest --test-dir '{self.build_folder}' --output-on-failure")

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
        if self.options.get_safe("prerelease"):
            self.cpp_info.defines.append("_PRERELEASE=1")
        if self.options.get_safe("sanitize") and self.options.sanitize != "False":
            if self.options.sanitize == "thread":
                self.cpp_info.sharedlinkflags.append("-fsanitize=thread")
                self.cpp_info.exelinkflags.append("-fsanitize=thread")
            else:
                self.cpp_info.sharedlinkflags.append("-fsanitize=address")
                self.cpp_info.exelinkflags.append("-fsanitize=address")
                self.cpp_info.sharedlinkflags.append("-fsanitize=undefined")
                self.cpp_info.exelinkflags.append("-fsanitize=undefined")

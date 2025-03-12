from os.path import join
from conan import ConanFile
from conan.tools.files import copy
from conans import CMake

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "8.8.2"
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

    generators = "cmake", "cmake_find_package"
    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*", "LICENSE"

    def configure(self):
        if self.options.shared:
            del self.options.fPIC
        if self.settings.build_type == "Debug":
            if self.options.sanitize:
                self.options['sisl'].sanitize = True
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
        if self.settings.arch != "x86_64":
            self.options["spdk"].native_build = True

    def build_requirements(self):
        self.build_requires("gtest/1.12.1")

    def requirements(self):
        self.requires("sisl/[~=8]")

        self.requires("boost/1.79.0")
        self.requires("folly/2022.01.31.00")
        self.requires("grpc/1.50.1")
        self.requires("grpc_internal/1.48.0")
        self.requires("liburing/2.1")
        self.requires("nlohmann_json/3.11.2")
        self.requires("libevent/2.1.12")
        self.requires("spdk/nbi.21.07.y")
        self.requires("pistache/nbi.0.0.5.1")

        self.requires("openssl/1.1.1s",     override=True)

    def build(self):
        cmake = CMake(self)
        definitions = {'CMAKE_TEST_TARGET': self.options.testing,
                       'CONAN_CMAKE_SILENT_OUTPUT': 'ON',
                       'CMAKE_EXPORT_COMPILE_COMMANDS': 'ON',
                       'MEMORY_SANITIZER_ON': 'OFF'}
        test_target = None

        run_tests = True
        if self.settings.build_type == "Debug":
            if self.options.sanitize:
                definitions['MEMORY_SANITIZER_ON'] = 'ON'
            elif self.options.coverage:
                definitions['BUILD_COVERAGE'] = 'ON'
                test_target = 'coverage'
            else:
                run_tests = False

        cmake.configure(defs=definitions)
        cmake.build()
        # Only test in Sanitizer mode, Coverage mode or Release mode
        if run_tests:
            cmake.test(target=test_target, output_on_failure=True)

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

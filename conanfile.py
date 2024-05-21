from conan import ConanFile
from conan.tools.files import copy, get, save, load
from conans import CMake
from os.path import join, exists
import json

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "11.3.2"

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
        "grpc_support": ['True', 'False'],
        "sanitize": ['True', 'False'],
        "spdk": ['True', 'False'],
        "testing" : ['full', 'off', 'epoll_mode', 'spdk_mode'],
        "fiber_impl" : ['boost', 'folly']
        }
    default_options = {
        'shared':       False,
        'fPIC':         True,
        'coverage':     False,
        'grpc_support': False,
        'sanitize':     False,
        'spdk':         True,
        'testing':      'epoll_mode',
        'fiber_impl':   'boost',
    }

    generators = "cmake", "cmake_find_package"
    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*", "LICENSE"

    def configure(self):
        if self.options.shared:
            del self.options.fPIC
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
            if self.options.testing == 'off':
                if self.options.coverage or self.options.sanitize:
                    raise ConanInvalidConfiguration("Coverage/Sanitizer requires Testing!")

    def build_requirements(self):
        self.build_requires("gtest/1.14.0")
        self.build_requires("cpr/1.10.4")

    def requirements(self):
        self.requires("sisl/[~12.2, include_prerelease=True]@oss/master")
        if self.options.grpc_support:
            self.requires("grpc/[>=1.50]")
        self.requires("liburing/2.4")
        if self.options.spdk:
            self.requires("spdk/21.07.y")
        self.requires("pistache/0.0.5")
        self.requires("openssl/3.1.3", override=True)
        self.requires("libcurl/8.4.0", override=True)
        self.requires("lz4/1.9.4", override=True)
        self.requires("zstd/1.5.5", override=True)

    def _download_grpc(self, folder):
        ref = self.dependencies['grpc'].ref.version
        source_info = self.dependencies['grpc'].conan_data["sources"][f"{ref}"]
        current_info_str = json.dumps(source_info, sort_keys=True)

        touch_file_path = join(folder, "grpc_download")

        if exists(touch_file_path) == False or load(self, touch_file_path) != current_info_str:
            print("-------------- downloading grpc sources ---------")
            get(self, **source_info, destination=join(folder, "grpc_internal"), strip_root=True)
            save(self, touch_file_path, current_info_str)

    def source(self):
        if self.options.grpc_support:
            self._download_grpc(self.source_folder)

    def generate(self):
        if self.options.grpc_support:
            self._download_grpc(self.source_folder)

    def build(self):
        cmake = CMake(self)
        definitions = {'CMAKE_TEST_TARGET': self.options.testing,
                       'CMAKE_EXPORT_COMPILE_COMMANDS': 'ON',
                       'CONAN_CMAKE_SILENT_OUTPUT': 'ON',
                       'MEMORY_SANITIZER_ON': 'OFF',
                       'BUILD_TESTING': 'OFF',
        }
        if self.options.testing:
            definitions['BUILD_TESTING'] = 'ON'

        if self.settings.build_type == "Debug":
            if self.options.sanitize:
                definitions['MEMORY_SANITIZER_ON'] = 'ON'
        if self.options.coverage:
            definitions['BUILD_COVERAGE'] = 'ON'

        if self.options.fiber_impl == "boost":
            definitions['FIBER_IMPL'] = 'boost'
        else:
            definitions['FIBER_IMPL'] = 'folly'

        cmake.configure(defs=definitions)
        cmake.build()
        if self.options.testing:
            cmake.test(output_on_failure=True)

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

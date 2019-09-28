from conans import ConanFile, CMake, tools

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "2.1.3"
    license = "Proprietary"
    url = "https://github.corp.ebay.com/SDS/iomgr"
    description = "iomgr"

    settings = "arch", "os", "compiler", "build_type", "sanitize"
    options = {
        "shared": ['True', 'False'],
        "fPIC": ['True', 'False'],
        }
    default_options = (
        'shared=False',
        'fPIC=True'
        )

    requires = (
            "libevent/2.1.8@bincrafters/stable",
            "sds_logging/5.3.2@sds/develop",
            "folly/2019.07.22.00@bincrafters/testing",
            "zstd/1.3.8@bincrafters/stable",
            "sisl/0.3.5@sisl/testing"
            )

    generators = "cmake"
    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*"

    def build(self):
        cmake = CMake(self)
        definitions = {'CMAKE_EXPORT_COMPILE_COMMANDS': 'ON',
                       'MEMORY_SANITIZER_ON': 'OFF'}
        cmake.configure(defs=definitions)
        cmake.build()

    def package(self):
        self.copy("*.h", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.hpp", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.cpp", dst="test/", src="test", keep_path=False)
        self.copy("*.a", dst="impl", keep_path=False)
        self.copy("*.so", dst="impl", keep_path=False)
        self.copy("*.dll", dst="impl", keep_path=False)
        self.copy("*.dylib", dst="impl", keep_path=False)
        self.copy("*.impl", dst="impl", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = tools.collect_libs(self)
        if self.settings.sanitize != None:
            self.cpp_info.sharedlinkflags.append("-fsanitize=address")
            self.cpp_info.exelinkflags.append("-fsanitize=address")

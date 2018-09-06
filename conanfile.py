from conans import ConanFile, CMake, tools

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "2.0.5"
    license = "Proprietary"
    description = "iomgr"

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True]}

    requires = (("libevent/2.0.22@bincrafters/stable"),
                ("sds_logging/3.2.2@sds/stable"))

    generators = "cmake"
    default_options = "shared=False", "fPIC=True"

    exports_sources = "src/*", "cmake/*", "CMakeLists.txt"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        self.copy("*.h", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.hpp", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)
        self.copy("*.so", dst="lib", keep_path=False)
        self.copy("*.dll", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)
        self.copy("*.lib", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = tools.collect_libs(self)

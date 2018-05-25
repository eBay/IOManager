from conans import ConanFile, CMake, tools

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "1.0.3"
    license = "Proprietary"
    description = "iomgr"

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True]}

    build_requires = (("sds_logging/[>=0.1.4,<1.0]@demo/dev"))

    generators = "cmake"
    default_options = "shared=False", "fPIC=True"
    exports_sources = "*"

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        self.copy("*.h", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.hpp", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)
        self.copy("*.so", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = tools.collect_libs(self)

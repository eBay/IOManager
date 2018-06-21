from conans import ConanFile, CMake, tools
from conans.tools import os_info

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "1.0.5"
    license = "Proprietary"
    description = "iomgr"

    settings = "os", "compiler", "build_type", "arch"
    options = {"shared": [True, False], "fPIC": [True]}

    requires = (("libevent/2.0.22@bincrafters/stable"),
                ("sds_logging/1.0.0@sds/stable"))

    generators = "cmake"
    default_options = "shared=False", "fPIC=True"
    exports_sources = "src/*", "cmake/*", "CMakeLists.txt"

    # These are not proper Conan dependencies, but support building
    # packages outside the official SDS build image. If you want to support
    # an OS/Platform that isn't listed, you'll need to add it yourself
    def system_requirements(self):
        pkgs = list()
        if os_info.linux_distro == "ubuntu":
            if os_info.os_version < "17":
                pkgs.append("g++-5")
            elif os_info.os_version < "18":
                pkgs.append("g++-6")
            elif os_info.os_version < "19":
                pkgs.append("g++-7")
            else:
                pkgs.append("g++")

        installer = tools.SystemPackageTool()
        for pkg in pkgs:
            installer.install(packages=pkg, update=False)

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

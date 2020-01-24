#!/usr/bin/env python
# -*- coding: utf-8 -*-
from conans import ConanFile, CMake, tools

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "3.0.5"
    revision_mode = "scm"
    license = "Proprietary"
    url = "https://github.corp.ebay.com/SDS/iomgr"
    description = "iomgr"

    settings = "arch", "os", "compiler", "build_type", "sanitize"
    options = {
        "shared": ['True', 'False'],
        "fPIC": ['True', 'False'],
        "coverage": ['True', 'False'],
        }
    default_options = (
        'shared=False',
        'fPIC=True',
        'coverage=False'
        )

    requires = (
            "folly/2019.09.23.00@bincrafters/stable",
            "libevent/2.1.11@bincrafters/stable",
            "sds_logging/6.1.0@sds/develop",
            ("zstd/1.4.0@bincrafters/stable", "override"),
            "sisl/0.3.16@sisl/develop",
            "OpenSSL/1.1.1c@conan/stable",
            )

    generators = "cmake"
    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*"

    def build(self):
        cmake = CMake(self)
        definitions = {'CMAKE_EXPORT_COMPILE_COMMANDS': 'ON',
                       'MEMORY_SANITIZER_ON': 'OFF'}
        test_target = None
                
        if self.settings.sanitize != "address" and self.options.coverage == 'True':
            definitions['CONAN_BUILD_COVERAGE'] = 'ON'
            test_target = 'coverage'

        cmake.configure(defs=definitions)
        cmake.build()
        cmake.test(target=test_target, output_on_failure=True)

    def package(self):
        self.copy("*.h", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.hpp", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)
        self.copy("*.so", dst="lib", keep_path=False)
        self.copy("*.dll", dst="lib", keep_path=False)
        self.copy("*.dylib", dst="lib", keep_path=False)

    def package_info(self):
        self.cpp_info.libs = tools.collect_libs(self)
        if self.settings.sanitize != None:
            self.cpp_info.sharedlinkflags.append("-fsanitize=address")
            self.cpp_info.exelinkflags.append("-fsanitize=address")
        elif self.options.coverage == 'True':
            self.cpp_info.libs.append('gcov')


#!/usr/bin/env python
# -*- coding: utf-8 -*-
from conans import ConanFile, CMake, tools

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "3.1.1"
    revision_mode = "scm"
    license = "Proprietary"
    url = "https://github.corp.ebay.com/SDS/iomgr"
    description = "iomgr"

    settings = "arch", "os", "compiler", "build_type"
    options = {
        "shared": ['True', 'False'],
        "fPIC": ['True', 'False'],
        "coverage": ['True', 'False'],
        "sanitize": ['True', 'False'],
        }
    default_options = (
        'shared=False',
        'fPIC=True',
        'coverage=False',
        'sanitize=False',
        )

    requires = (
            "sds_logging/7.0.0@sds/develop",
            "sisl/1.0.3@sisl/develop",
            "boost/1.72.0",
            "folly/2020.03.02.00",
            "libevent/2.1.11",
            )

    generators = "cmake"
    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*"

    def configure(self):
        if self.settings.compiler != "gcc" or self.options.sanitize:
            self.options.coverage = False

    def build(self):
        cmake = CMake(self)
        definitions = {'CMAKE_EXPORT_COMPILE_COMMANDS': 'ON',
                       'MEMORY_SANITIZER_ON': 'OFF'}
        test_target = None

        if self.options.sanitize:
            definitions['MEMORY_SANITIZER_ON'] = 'ON'
                
        if self.options.coverage:
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
        if self.options.sanitize:
            self.cpp_info.sharedlinkflags.append("-fsanitize=address")
            self.cpp_info.exelinkflags.append("-fsanitize=address")
            self.cpp_info.sharedlinkflags.append("-fsanitize=undefined")
            self.cpp_info.exelinkflags.append("-fsanitize=undefined")
        elif self.options.coverage == 'True':
            self.cpp_info.libs.append('gcov')


#!/usr/bin/env python
# -*- coding: utf-8 -*-
from conans import ConanFile, CMake, tools

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "8.4.15"

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
        "testing" : ['full', 'off', 'epoll_mode', 'spdk_mode'],
        "prerelease": ['True', 'False'],
        }
    default_options = (
        'shared=False',
        'fPIC=True',
        'coverage=False',
        'sanitize=False',
        'testing=full',
        'prerelease=True',
        )

    requires = (
            "flip/[~=3, include_prerelease=True]@sds/master",
            "sisl/[~=7, include_prerelease=True]@sisl/master",

            "boost/1.73.0",
            "grpc/1.37.0",
            ("fmt/8.0.1", "override"),
            "folly/2020.05.04.00",
            "nlohmann_json/3.8.0",
            "libevent/2.1.11",
            "spdk/21.07.x",
            "openssl/1.1.1k",
            "isa-l/2.21.0",
            "semver/1.1.0",
            "grpc_internal/1.37.0",
            "liburing/2.1"
            )
    build_requires = (
                "gtest/1.10.0",
                )

    generators = "cmake"
    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*"

    def config_options(self):
        if self.settings.build_type != "Debug":
            del self.options.sanitize
            del self.options.coverage

    def configure(self):
        self.options['sisl'].prerelease = self.options.prerelease
        self.options['flip'].prerelease = self.options.prerelease
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
            if self.options.coverage or self.options.sanitize:
                self.options['sisl'].malloc_impl = 'libc'

    def build(self):
        cmake = CMake(self)
        definitions = {'CONAN_TEST_TARGET': self.options.testing,
                       'CMAKE_EXPORT_COMPILE_COMMANDS': 'ON',
                       'MEMORY_SANITIZER_ON': 'OFF'}
        test_target = None

        run_tests = True
        if self.settings.build_type == "Debug":
            if self.options.sanitize:
                definitions['MEMORY_SANITIZER_ON'] = 'ON'
            elif self.options.coverage:
                definitions['CONAN_BUILD_COVERAGE'] = 'ON'
                test_target = 'coverage'
            else:
                run_tests = False

        cmake.configure(defs=definitions)
        cmake.build()
        # Only test in Sanitizer mode, Coverage mode or Release mode
        if run_tests:
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
        self.cpp_info.cxxflags.append("-fconcepts")
        if self.settings.build_type == "Debug":
            if  self.options.sanitize:
                self.cpp_info.sharedlinkflags.append("-fsanitize=address")
                self.cpp_info.exelinkflags.append("-fsanitize=address")
                self.cpp_info.sharedlinkflags.append("-fsanitize=undefined")
                self.cpp_info.exelinkflags.append("-fsanitize=undefined")
            elif self.options.coverage == 'True':
                self.cpp_info.libs.append('gcov')

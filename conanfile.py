from conans import ConanFile, CMake, tools

class IOMgrConan(ConanFile):
    name = "iomgr"
    version = "8.6.3"
    homepage = "https://github.corp.ebay.com/SDS/iomgr"
    description = "Asynchronous event manager"
    topics = ("ebay", "nublox")
    url = "https://github.corp.ebay.com/SDS/iomgr"
    license = "Proprietary"

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
        'testing':      'full',
        'sisl:prerelease':   True,
    }


    generators = "cmake", "cmake_find_package"
    exports_sources = "CMakeLists.txt", "cmake/*", "src/*", "test/*"

    def config_options(self):
        if self.settings.build_type != "Debug":
            del self.options.sanitize
            del self.options.coverage

    def configure(self):
        if self.options.shared:
            del self.options.fPIC
        if self.settings.build_type == "Debug":
            if self.options.coverage and self.options.sanitize:
                raise ConanInvalidConfiguration("Sanitizer does not work with Code Coverage!")
            if self.options.coverage or self.options.sanitize:
                self.options['sisl'].malloc_impl = 'libc'

    def build_requirements(self):
        self.build_requires("gtest/1.11.0")

    def requirements(self):
        self.requires("flip/[~=4, include_prerelease=True]@sds/master")
        self.requires("sisl/[~=8, include_prerelease=True]@sisl/master")

        self.requires("boost/1.79.0")
        self.requires("folly/2022.01.31.00")
        self.requires("grpc/1.48.0")
        self.requires("grpc_internal/1.48.0")
        self.requires("liburing/2.1")
        self.requires("nlohmann_json/3.10.5")
        self.requires("libevent/2.1.12")
        self.requires("spdk/21.07.x")
        self.requires("evhtp/1.2.18.2")

        self.requires("flatbuffers/1.12.0", override=True)
        self.requires("openssl/1.1.1q", override=True)
        self.requires("zlib/1.2.12", override=True)

    def build(self):
        cmake = CMake(self)
        definitions = {'CMAKE_TEST_TARGET': self.options.testing,
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
        self.copy("*.h", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.hpp", dst="include/iomgr", src="src", keep_path=False)
        self.copy("*.a", dst="lib", keep_path=False)
        self.copy("*.so", dst="lib", keep_path=False, symlinks=True)
        self.copy("*.dylib", dst="lib", keep_path=False, symlinks=True)
        self.copy("*.dll", dst="lib", keep_path=False)

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

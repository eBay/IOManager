from conan import ConanFile
from conan.tools.gnu import Autotools, AutotoolsToolchain
from conan.tools.files import get, copy
from os.path import join

required_conan_version = ">=1.60.0"

class LibFIOConan(ConanFile):
    name = "fio"
    description = "Flexible IO Kit"
    url = "https://github.corp.ebay.com/conan/fio"
    homepage = "https://github.com/axboe/fio"
    license = "GPL-2"
    settings = "os", "arch", "compiler"
    options = {
        "native_build": ['True', 'False'],
        }
    default_options = {
        "native_build": False,
    }

    def configure(self):
        del self.settings.compiler.libcxx

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        tc = AutotoolsToolchain(self)
        tc.configure_args = []
        if not self.options.native_build:
            tc.configure_args.append("--disable-native")
        tc.generate()

    def build(self):
        autotools = Autotools(self)
        autotools.configure()
        autotools.make()

    def package(self):
        copy(self, "fio", self.build_folder, join(self.package_folder, "bin"), keep_path=False)
        copy(self, "*.h", self.source_folder, dst=join(self.package_folder, "include/fio"), keep_path=True)

    def deploy(self):
        self.copy("fio", dst="/usr/local/bin/", src="bin")

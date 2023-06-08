#!/usr/bin/env python
# -*- coding: utf-8 -*-

from conans import ConanFile, AutoToolsBuildEnvironment
from conans import tools
import os

class LibFIOConan(ConanFile):
    name = "fio"
    version = "3.28"
    description = "Flexible IO Kit"
    url = "https://github.corp.ebay.com/conan/fio"
    homepage = "https://github.com/axboe/fio"
    license = "GPL-2"
    exports = ["arch.patch", "rm_raw.patch"]
    settings = "os", "arch", "compiler"
    options = {
        "native_build": [True, False],
        }
    default_options = (
        "native_build=False",
        )
    source_subfolder = "source_subfolder"

    def configure(self):
        del self.settings.compiler.libcxx

    def source(self):
        tools.get("{0}/archive/fio-{1}.tar.gz".format(self.homepage, self.version))
        os.rename("%s-%s-%s" % (self.name, self.name, self.version), self.source_subfolder)
#        tools.patch(strip=0, base_path=self.source_subfolder, patch_file="arch.patch")
#        tools.patch(strip=0, base_path=self.source_subfolder, patch_file="rm_raw.patch")

    def build(self):
        autotools = AutoToolsBuildEnvironment(self)
        cargs = []
        if not self.options.native_build:
            cargs.append("--disable-native")
        env_vars = autotools.vars
        with tools.environment_append(env_vars):
            with tools.chdir(self.source_subfolder):
                autotools.configure(args=cargs)
                autotools.make()

    def package(self):
        self.copy("fio", dst="bin", src="{}/".format(self.source_subfolder), keep_path=False)
        self.copy("*.h", dst="include/fio", src="{}/".format(self.source_subfolder), keep_path=True)

    def deploy(self):
        self.copy("fio", dst="/usr/local/bin/", src="bin")

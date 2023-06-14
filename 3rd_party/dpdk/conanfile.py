#!/usr/bin/env python
# -*- coding: utf-8 -*-

from conans import ConanFile, tools, Meson
from conans.tools import os_info, SystemPackageTool
import glob, os

class LibDPDKConan(ConanFile):
    name = "dpdk"
    version = "21.05"
    description = "Data Plane Development Kit"
    url = "https://github.corp.ebay.com/conan/dpdk"
    homepage = "https://github.com/dpdk/dpdk"
    license = "BSD-3"
    generators = "pkg_config"

    exports = ["LICENSE.md", "numa.patch"]
    source_subfolder = "source_subfolder"
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "numa": [True, False],
        "native_build": [True, False],
    }
    default_options = (
        "shared=False",
        "fPIC=True",
        "numa=False",
        "native_build=False",
    )
    scm = {"type"       : "git",
           "subfolder"  : source_subfolder,
           "url"        : "https://github.com/spdk/dpdk.git",
           "revision"   : "bad3c0e51d7a34e3188d75d94f15a36c8f5e8301"}

    build_requires = (
                        "meson/0.59.0",
                        )

    def configure(self):
        del self.settings.compiler.libcxx

    def source(self):
        tools.patch(strip=0, base_path=self.source_subfolder, patch_file="numa.patch")

    def build(self):
        meson = Meson(self)
        meson_options = {
                    'machine':                  'default',
                    'use_numa':                 'false',
                    }
        if self.options.native_build:
            meson_options['machine'] = 'native'
        if self.options.numa:
            meson_options['use_numa'] = 'true'
        meson.configure(build_folder="build", defs=meson_options, source_folder="{}/{}".format(self.source_folder, self.source_subfolder))
        meson.build()

    def package(self):
        meson = Meson(self)
        meson.install(build_dir="build")

        removed_libs = glob.glob("{}/lib/*.a".format(self.package_folder), recursive=True)
        if not self.options.shared:
            removed_libs = glob.glob("{}/**/*.so*".format(self.package_folder), recursive=True)
        for f in removed_libs:
            os.remove(f)
        if not self.options.shared:
            os.removedirs("{}/lib/dpdk/pmds-21.2".format(self.package_folder))

    def package_info(self):
        self.cpp_info.libs = [
                              "-Wl,--whole-archive -lrte_eal",
                              "rte_timer",
                              "rte_power",
                              "rte_mempool",
                              "rte_ring",
                              "rte_mbuf",
                              "rte_mempool_ring",
                              "rte_telemetry",
                              "rte_bus_pci",
                              "rte_pci",
                              "rte_kvargs",
                              "rte_net",
                              "-lrte_cryptodev",
                              "-lrte_ethdev",
                              "-lrte_rcu -Wl,--no-whole-archive"
                              ]
        if self.options.numa:
            self.cpp_info.libs.append("numa")
        if self.settings.os == "Linux":
            self.cpp_info.libs.extend(["pthread", "dl"])
        self.env_info.RTE_SDK = self.package_folder

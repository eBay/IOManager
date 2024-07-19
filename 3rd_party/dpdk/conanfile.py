from conan import ConanFile
from conan.tools.meson import Meson, MesonToolchain
from conan.tools.files import patch, get

import glob, os
from os.path import join

required_conan_version = ">=1.60.0"

class LibDPDKConan(ConanFile):
    name = "dpdk"
    description = "Data Plane Development Kit"
    url = "https://github.corp.ebay.com/conan/dpdk"
    homepage = "https://github.com/dpdk/dpdk"
    license = "BSD-3"

    source_subfolder = "source_subfolder"
    settings = "os", "compiler", "build_type", "arch"
    options = {
        "shared": ['True', 'False'],
        "fPIC": ['True', 'False'],
        "numa": ['True', 'False'],
        "native_build": ['True', 'False'],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "numa": False,
        "native_build": False,
    }

    exports_sources = ("LICENSE.md", "numa.patch")
    no_copy_source=True

    def build_requirements(self):
        self.tool_requires("meson/0.59.3")

    def configure(self):
        del self.settings.compiler.libcxx

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)
        patch_file = os.path.join(self.export_sources_folder, "numa.patch")
        patch(self, patch_file=patch_file)

    def generate(self):
        tc = MesonToolchain(self)
        tc.project_options["machine"] = "generic"
        tc.preprocessor_definitions["use_numa"] = "false"
        if self.options.native_build:
            tc.project_options['machine'] = 'native'
        if self.options.numa:
            tc.preprocessor_definitions['use_numa'] = 'true'
        tc.generate()

    def build(self):
        meson = Meson(self)
        meson.configure()
        meson.build()

    def package(self):
        meson = Meson(self)
        meson.install()

        removed_libs = glob.glob("{}/lib/*.a".format(self.package_folder), recursive=True)
        if not self.options.shared:
            removed_libs = glob.glob("{}/**/*.so*".format(self.package_folder), recursive=True)
        for f in removed_libs:
            os.remove(f)
        if not self.options.shared:
            os.removedirs("{}/lib/dpdk/pmds-21.2".format(self.package_folder))

    def package_info(self):
        self.cpp_info.libs = [
                              "rte_eal",
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
                              "rte_cryptodev",
                              "rte_ethdev",
                              "rte_rcu"
                              ]
        if self.options.numa:
            self.cpp_info.libs.append("numa")
        if self.settings.os == "Linux":
            self.cpp_info.system_libs.extend(["pthread", "dl"])
        self.env_info.RTE_SDK = self.package_folder

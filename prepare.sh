set -eu

echo -n "Installing ELF tools..."
sudo apt-get install -y python3-pyelftools libaio-dev > /dev/null
#pipx install pyelftools > /dev/null
python -m pip install pyelftools > /dev/null
echo "done."
echo -n "Exporting custom recipes..."
echo -n "dpdk."
conan export --version nbi.21.05 3rd_party/dpdk 
echo -n "fio."
conan export --version nbi.3.28 3rd_party/fio
echo -n "spdk."
conan export --version nbi.21.07.y 3rd_party/spdk
echo -n "pistache."
conan export 3rd_party/pistache pistache/nbi.0.0.5.1@

echo "done."

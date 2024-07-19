set -eu

echo -n "Installing ELF tools..."
sudo apt-get install -y python3-pyelftools libaio-dev > /dev/null
#pipx install pyelftools > /dev/null
python -m pip install pyelftools > /dev/null
echo "done."
echo -n "Exporting custom recipes..."
echo -n "dpdk."
conan export 3rd_party/dpdk dpdk/21.05@
echo -n "fio."
conan export 3rd_party/fio
echo -n "spdk."
conan export 3rd_party/spdk

echo "done."

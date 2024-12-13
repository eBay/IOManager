set -eu

echo -n "Installing ELF tools..."
sudo apt-get install -y python3-pyelftools libaio-dev > /dev/null
#pipx install pyelftools > /dev/null
python -m pip install pyelftools > /dev/null
echo "done."
echo -n "Exporting custom recipes..."
echo -n "dpdk."
conan export 3rd_party/dpdk dpdk/nbi.21.05@
echo -n "fio."
conan export 3rd_party/fio fio/nbi.3.28@
echo -n "spdk."
conan export 3rd_party/spdk spdk/nbi.21.07.y@
echo -n "grpc_internal."
conan export 3rd_party/grpc_internal
echo -n "pistache."
conan export 3rd_party/pistache pistache/nbi.0.0.5.1@

echo "done."

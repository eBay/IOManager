set -eu

echo -n "Installing ELF tools..."
sudo apt-get install -y python3-pyelftools libaio-dev > /dev/null
#pipx install pyelftools > /dev/null
python -m pip install pyelftools > /dev/null
echo "done."
echo -n "Exporting custom recipes..."
echo -n "pistache."
conan export 3rd_party/pistache --name pistache --version nbi.0.0.5.1 >/dev/null
echo "done."

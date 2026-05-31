set -eu

echo -n "Installing ELF tools..."
sudo apt-get install -y python3-pyelftools > /dev/null
#pipx install pyelftools > /dev/null
python -m pip install pyelftools > /dev/null
echo "done."

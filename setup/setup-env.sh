#!/bin/bash
#!/bin/bash

mode="$1"
ubuntu_version=$(lsb_release -r -s)

if [ $ubuntu_version == "18.04" ]; then
  wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz
  mv MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu18.04-x86_64.tgz ofed.tgz
elif [ $ubuntu_version == "20.04" ]; then
  wget https://content.mellanox.com/ofed/MLNX_OFED-4.9-5.1.0.0/MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu20.04-x86_64.tgz
  mv MLNX_OFED_LINUX-4.9-5.1.0.0-ubuntu20.04-x86_64.tgz ofed.tgz
else
  echo "Wrong ubuntu distribution for $mode!"
  exit 0
fi
echo $mode $ubuntu_version $ofed_fid

sudo apt update -y

# install anaconda
mkdir install
mv ofed.tgz install

cd install
if [ ! -f "./anaconda-install.sh" ]; then
  wget https://repo.anaconda.com/archive/Anaconda3-2022.05-Linux-x86_64.sh -O anaconda-install.sh
fi
if [ ! -d "$HOME/anaconda3" ]; then
  chmod +x anaconda-install.sh
  ./anaconda-install.sh -b
  export PATH=$PATH:$HOME/anaconda3/bin
  # add conda to path
  echo PATH=$PATH:$HOME/anaconda3/bin >> $HOME/.bashrc
  conda init
  source ~/.bashrc
  # activate base
fi
conda activate base
cd ..

pip install gdown
sudo apt install memcached -y
sudo apt install libtbb-dev libboost-all-dev -y

# install ofed
cd install
if [ ! -d "./ofed" ]; then
  tar zxf ofed.tgz
  mv MLNX* ofed
fi
cd ofed
sudo ./mlnxofedinstall --force
if [ $mode == "scalestore" ]; then
  sudo /etc/init.d/openibd restart
fi
cd ..

# install cmake
cd install
if [ ! -f cmake-3.16.8.tar.gz ]; then
  wget https://cmake.org/files/v3.16/cmake-3.16.8.tar.gz
fi
if [ ! -d "./cmake-3.16.8" ]; then
  tar zxf cmake-3.16.8.tar.gz
  cd cmake-3.16.8 && ./configure && make -j 4 && sudo make install
fi
cd ..

# install gtest
if [ ! -d "/usr/src/gtest" ]; then
  sudo apt install -y libgtest-dev
fi
cd /usr/src/gtest
sudo cmake .
sudo make
sudo make install
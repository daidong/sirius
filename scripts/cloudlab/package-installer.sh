#!/usr/bin/env bash

sudo apt-get update
sudo apt-get install -y openjdk-7-jdk
sudo apt-get install -y thrift-compiler
sudo apt-get install -y maven
sudo apt-get install -y libgflags-dev
sudo apt-get install -y libsnappy-dev

cd ~/
git clone https://github.com/daidong/simplegdb-Java.git
cd ~/simplegdb-Java
make all

mkdir -p ~/dbs/

echo "export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64/" >> ~/.bashrc
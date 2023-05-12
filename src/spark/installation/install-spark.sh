#!/bin/bash

sudo apt install default-jdk -y

sudo apt install scala -y

wget https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
sudo tar -xvzf spark-3.3.1-bin-hadoop3.tgz
sudo mv spark-3.3.1-bin-hadoop3 /mnt/spark
sudo chmod 777 /mnt/spark

echo "export SPARK_HOME=/mnt/spark" >> ~/.bashrc
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bashrc

source ~/.bashrc

#rm spark-3.3.1-bin-hadoop3.tgz

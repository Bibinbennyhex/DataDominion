#!/bin/bash
# =============================================================================
# EMR Bootstrap Script for Summary Pipeline v9.1
# =============================================================================
# This script runs on each node during EMR cluster creation

set -e

echo "Starting bootstrap script..."

# Install Python dependencies
sudo pip3 install python-dateutil pyarrow

# Tune OS for Spark workloads
echo "Tuning OS settings..."
sudo sysctl -w vm.swappiness=1
sudo sysctl -w net.core.somaxconn=65535
sudo sysctl -w net.ipv4.tcp_max_syn_backlog=65535

# Increase file descriptors
echo "* soft nofile 1000000" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 1000000" | sudo tee -a /etc/security/limits.conf

# Optimize EBS volumes for shuffle (if NVMe volumes exist)
for disk in /dev/nvme1n1 /dev/nvme2n1 /dev/nvme3n1 /dev/nvme4n1; do
    if [ -b "$disk" ]; then
        echo "Optimizing $disk..."
        sudo blockdev --setra 2048 $disk 2>/dev/null || true
    fi
done

# Configure Hadoop tmp directories on multiple volumes for better I/O
if [ -d "/mnt1" ] && [ -d "/mnt2" ]; then
    echo "Configuring multi-volume tmp directories..."
    sudo mkdir -p /mnt1/spark /mnt2/spark
    sudo chmod 1777 /mnt1/spark /mnt2/spark
fi

echo "Bootstrap complete!"

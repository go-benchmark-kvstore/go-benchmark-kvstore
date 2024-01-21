#!/bin/bash -e

if [[ -z "$XFS_RUNNER_TOKEN" && -z "$EXT4_RUNNER_TOKEN" ]]; then
  echo "missing XFS_RUNNER_TOKEN and/or EXT4_RUNNER_TOKEN environment variables"
  exit 1
fi

export RESOURCE_GROUP_NAME=kv-benchmark
export LOCATION=eastus
export SIZE=Standard_L32as_v3
export PARTITIONS="/dev/nvme0n1p1 /dev/nvme1n1p1 /dev/nvme2n1p1 /dev/nvme3n1p1"
export OS_DISK_SIZE=319 # Temporary storage size for Standard_L32as_v3.
export VM_NAME=runner
export VM_IMAGE=Ubuntu2204
export ADMIN_USERNAME=benchmark
export VM_COUNT=20

name="${VM_NAME}0"
count_arg=""
if [[ "$VM_COUNT" -gt 1 ]]; then
  name="$VM_NAME"
  count_arg="--count $VM_COUNT"
fi

set -x

az group create --name "$RESOURCE_GROUP_NAME" --location "$LOCATION"

az vm create \
  --resource-group "$RESOURCE_GROUP_NAME" \
  --name "$name" \
  --image "$VM_IMAGE" \
  --size "$SIZE" \
  --admin-username "$ADMIN_USERNAME" \
  --ssh-key-values ~/.ssh/id_rsa.pub \
  --ephemeral-os-disk true \
  --ephemeral-os-disk-placement ResourceDisk \
  --os-disk-caching ReadOnly \
  --os-disk-size-gb "$OS_DISK_SIZE" \
  $count_arg

sleep 3

for i in $(seq 0 "$(($VM_COUNT-1))") ; do
  export IP_ADDRESS="$(az vm show --show-details --resource-group "$RESOURCE_GROUP_NAME" --name "$VM_NAME$i" --query publicIps --output tsv)"

  ssh-keyscan "$IP_ADDRESS" >> ~/.ssh/known_hosts

  ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" "curl -fsSL https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh | sudo bash"
  ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" "curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg"
  ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" sudo chmod a+r /etc/apt/keyrings/docker.gpg
  # TODO: "jammy" is hard-coded here, but it should depend on $VM_IMAGE (or be obtained while running remotely).
  ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" "echo 'deb [arch=amd64 signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu jammy stable' | sudo tee /etc/apt/sources.list.d/docker.list"
  ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" sudo apt-get update -q -q
  ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" sudo apt-get upgrade --yes --force-yes
  ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" sudo apt-get install --yes --force-yes docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin gitlab-runner xfsprogs
  for partition in $PARTITIONS ; do
    ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" "echo 'type=83' | sudo sfdisk ${partition%p1}"
  done
  ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" "sudo docker plugin install --alias mkfs --grant-all-permissions registry.gitlab.com/go-benchmark-kvstore/docker-volume-mkfs/plugin-branch/main:latest args='-l debug --logging.console.type=nocolor $PARTITIONS'"
  if [[ ! -z "$XFS_RUNNER_TOKEN" ]]; then
    ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" sudo gitlab-runner register --non-interactive --url "https://gitlab.com" --token "$XFS_RUNNER_TOKEN" --executor "docker" --docker-image ruby:3.1 --docker-ulimit nofile:1048576 --docker-disable-cache --docker-volume-driver mkfs --docker-volume-driver-ops fs:xfs
  fi
  if [[ ! -z "$EXT4_RUNNER_TOKEN" ]]; then
    ssh -l "$ADMIN_USERNAME" "$IP_ADDRESS" sudo gitlab-runner register --non-interactive --url "https://gitlab.com" --token "$EXT4_RUNNER_TOKEN" --executor "docker" --docker-image ruby:3.1 --docker-ulimit nofile:1048576 --docker-disable-cache --docker-volume-driver mkfs --docker-volume-driver-ops fs:ext4
  fi
done

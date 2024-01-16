#!/bin/bash -e

export RESOURCE_GROUP_NAME=kv-benchmark

az group delete --name "$RESOURCE_GROUP_NAME" --yes --force-deletion-types Microsoft.Compute/virtualMachines

#!/bin/bash

for env_name in "$@"
do
    echo "$env_name"=`printf '%s\n' "${!env_name}"`
done
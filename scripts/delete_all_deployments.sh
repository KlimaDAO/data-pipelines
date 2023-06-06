#!/bin/bash
set -e
prefect deployment ls | awk '{print $2}' | xargs -I {}  prefect deployment delete {}

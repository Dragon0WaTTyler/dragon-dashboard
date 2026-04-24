#!/bin/bash
set -euo pipefail

cd ~/Dragon
git pull
git log -1 --oneline

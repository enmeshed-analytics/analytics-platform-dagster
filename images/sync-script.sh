#!/bin/bash
#
# Exit script on any error
set -e

# Check for env var
if [ -z "$GIT_REPO_URL" ]; then
  echo "GIT_REPO_URL environment variable is not set."
  exit 1
fi

# Function to retry a command up to 5 times
retry() {
  local n=1
  local max=5
  local delay=2
  while true; do
    "$@" && break || {
      if [[ $n -lt $max ]]; then
        ((n++))
        echo "Command failed. Attempt $n/$max:"
        sleep $delay;
      else
        echo "The command has failed after $n attempts."
        return 1
      fi
    }
  done
}

# Clone repo
if [ ! -d ".git" ]; then
  echo "Cloning Git repository..."
  retry git clone $GIT_REPO_URL .
fi

# Run git pull every 30 seconds
while true
do
  echo "Pulling from Git repository..."
  retry git pull
  echo "Pull completed. Waiting for 30 seconds..."
  sleep 30
done

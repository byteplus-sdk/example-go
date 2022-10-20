#!/bin/bash
set -e
export https_proxy=http://${PROXY_HOST}:${PROXY_PORT} \
       http_proxy=http://${PROXY_HOST}:${PROXY_PORT} \
       no_proxy="*.byted.org"

# 拉取github仓库
Repo=$(echo $CI_REPO_NAME|awk '{split($1,arr,"/"); print arr[2]}')

git branch -v
git remote add origin-git  https://${GIT_NAME}:${GIT_TOKEN}@github.com/byteplus-sdk/${Repo}.git
git remote -v
git tag -l
git checkout -b main
git push origin-git main
git push origin-git --tags

echo "Sync Success!"
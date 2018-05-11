#!/bin/bash
prefix=MPDK_uNVMe
file=../MPDK_uNVMe_SDK.tgz
cd ..
git archive --format=tar --prefix=$prefix/ HEAD | gzip > "$file"
cd -

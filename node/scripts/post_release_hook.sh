#!/bin/bash

set -x

cd _build/default/rel/mzbench/releases && \
  for i in $(find . -maxdepth 1 ! -path . -type d); do ln -s start.boot $i/mzbench.boot ; done



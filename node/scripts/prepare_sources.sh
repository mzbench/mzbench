#!/bin/bash

set -x

rm -rf prepared_sources
rsync -aWL --exclude=_checkouts --exclude prepared_sources . prepared_sources/
mkdir -p prepared_sources/_checkouts

for i in $(find _checkouts -maxdepth 1 -type l); do cp -r ./_checkouts/`readlink $i` prepared_sources/$i ; done
for i in $(find _checkouts -maxdepth 1 ! -path _checkouts -type d); do cp -r $i prepared_sources/$i ; done


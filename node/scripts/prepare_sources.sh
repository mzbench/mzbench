#!/bin/bash

set -x

rm -rf prepared_sources
rsync -aWL --exclude=_checkouts --exclude prepared_sources . prepared_sources/
mkdir -p prepared_sources/_checkouts

for i in $(ls _checkouts); do cp -r ./_checkouts/`readlink _checkouts/$i` prepared_sources/_checkouts/$i ; done

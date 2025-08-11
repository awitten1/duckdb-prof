#!/bin/bash

set -eux

build_jemalloc() {
  local JEMALLOC_VERSION=5.3.0
  mkdir -p deps
  mkdir -p deps/source
  mkdir -p deps/build
  mkdir -p deps/install

  local file_name="jemalloc-${JEMALLOC_VERSION}.tar.bz2"

  (cd deps/source &&
    wget https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VERSION}/$file_name &&
    tar -xvjf $file_name && cd ${file_name%.tar.bz2} &&
    ./configure --enable-prof --prefix=$(realpath ../../install) && make -j14 && make install)


}

build_jemalloc


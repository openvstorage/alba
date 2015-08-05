#!/bin/bash

eval `opam config env`

DEB_BUILD_OPTIONS=nostrip fakeroot debian/rules clean build binary

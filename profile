#!/bin/sh
python2 -m cProfile -s time -o "${1}.prof" "${1}"

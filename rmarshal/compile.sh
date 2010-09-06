#!/bin/sh

gcc -I/usr/lib/ruby/1.8/i486-linux/ -lruby1.8 --shared rmarshal.c  -o rmarshal.so


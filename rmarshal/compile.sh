#!/bin/sh

gcc -I/usr/lib/ruby/1.8/i486-linux/ -lruby1.8 --shared rmarshal-1.8.c  -o rmarshal-1.8.so


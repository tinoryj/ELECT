#!/bin/bash

export JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH

# Generate libec.so
gcc -I ${JAVA_HOME}/include/linux/ -I ${JAVA_HOME}/include/ -I /usr/includ \
    -Wall -g -fPIC -shared -o libec.so \
    jni_common.c erasure_coder.c dump.c \
    NativeRSEncoder.c NativeRSDecoder.c \
    -L/usr/lib -lisal
echo "Complete generating libec.so!"

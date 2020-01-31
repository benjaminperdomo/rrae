#!/bin/bash

BASE_FOLDER=../cc-output/

test -d input || mkdir input

if [ -e input/warc_all.txt ]; then
	echo "Archivo input/warc_all.txt ya existe"
	echo "Por favor borre todo el contenido de input para generar de nuevo los archivos"
	exit 1
fi

for f in ../cc-output/warc/*; do
    if [ -d "$f" ]; then
        find $f -maxdepth 1 -name "*.warc" | xargs realpath >>input/warc_$(basename $f).txt
        find $f -maxdepth 1 -name "*.warc" | xargs realpath >>input/warc_all.txt
    fi
done
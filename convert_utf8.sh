#!/bin/bash

cd data
for file in *.csv
do
	iconv -f ISO-8859-1 "$file" -t UTF-8 -o "UTF8_$file"
	rm "$file"
	mv "UTF8_$file" "$file"
done
cd ..
#!/bin/bash

dir=/extract_files

inotifywait -m "$dir" --format '%w%f' -e create |
while read file
do
    ./upload_file.sh "$file" &
done
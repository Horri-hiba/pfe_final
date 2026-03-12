#!/bin/bash

apt-get update
apt-get install -y unzip

echo "Listing ZIP files..."

find /data -type f -name "*.zip" | while read file; do
  echo "Extracting $file"
  unzip -o "$file" -d "$(dirname "$file")"
done

echo "Extraction finished."
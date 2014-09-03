#! /bin/bash

echo "Usage: start_client.sh lookup (for looking up id )"
echo "       start_client.sh store xxxx (for store value xxxx to id)"
base_dir=$(dirname $0)
 java -cp $base_dir/../lib/Pistachios.jar:$base_dir/../lib/*:$base_dir/../config/ -Dlog4j.configuration=file://$base_dir/../config/log4j.xml com.yahoo.ads.pb.PistachiosClient $*

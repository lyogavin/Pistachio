#! /bin/bash
base_dir=$(dirname $0)
echo "java -cp $base_dir/../lib/*:$base_dir/../config/ -Djava.library.path=$base_dir/../lib -Dlog4j.configuration=$base_dir/../config/log4j.xml com.yahoo.ads.pb.PistachiosServer"
nohup java -cp $base_dir/../lib/*:$base_dir/../config/ -Djava.library.path=$base_dir/../lib -Dlog4j.configuration=$base_dir/../config/log4j.xml com.yahoo.ads.pb.PistachiosServer &

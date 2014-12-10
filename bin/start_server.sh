#! /bin/bash
base_dir=$(readlink -f $(dirname $0))
echo "java -cp $base_dir/../lib/*:$base_dir/../config/ -Djava.library.path=$base_dir/../lib -Dlog4j.configuration=$base_dir/../config/log4j.xml com.yahoo.ads.pb.PistachiosServer"
nohup java -cp $base_dir/../lib/*:$base_dir/../config/ -Djava.library.path=$base_dir/../lib -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9999 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Dlog4j.configuration=file://$base_dir/../config/log4j.xml -DconfigPath=$base_dir/../config com.yahoo.ads.pb.PistachiosServer &

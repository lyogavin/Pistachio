#! /bin/bash
base_dir=$(readlink -f $(dirname $0))
 echo "example: ./format_cluster.sh mbus0001.pbp.sg3.yahoo.com:2181 druid0001.pbp.sg3.yahoo.com,druid0002.pbp.sg3.yahoo.com,druid0003.pbp.sg3.yahoo.com,druid0003.pbp.sg3.yahoo.com,druid0009.pbp.sg3.yahoo.com,druid0010.pbp.sg3.yahoo.com 256 2 [kafka8Agg]"
 java -cp $base_dir/../lib/*:$base_dir/../config/ -Djava.library.path=$base_dir/../lib -Dlog4j.configuration=file://$base_dir/../config/log4j.xml com.yahoo.ads.pb.PistachiosFormatter $*

#! /bin/bash
 echo "example: ./format_cluster.sh mbus0001.pbp.sg3.yahoo.com:2181 druid0001.pbp.sg3.yahoo.com,druid0002.pbp.sg3.yahoo.com,druid0003.pbp.sg3.yahoo.com,druid0003.pbp.sg3.yahoo.com,druid0009.pbp.sg3.yahoo.com,druid0010.pbp.sg3.yahoo.com 256 2"
 java -cp ./Pistachios.jar:./lib/*:~/ -Djava.library.path=./native -Dlog4j.configuration=./conf/log4j.xml com.yahoo.ads.pb.PistachiosFormatter $*

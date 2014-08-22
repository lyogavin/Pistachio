#! /bin/bash

echo "Usage: start_client.sh lookup (for looking up id )"
echo "       start_client.sh store xxxx (for store value xxxx to id)"
 java -cp ./Pistachios.jar:./lib/*:~/ -Dlog4j.configuration=./conf/log4j.xml com.yahoo.ads.pb.PistachiosClient $*

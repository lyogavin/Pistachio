#! /bin/bash
 java -cp ./Pistachios.jar:./lib/*:~/ -Djava.library.path=./native -Dlog4j.configuration=./conf/log4j.xml com.yahoo.ads.pb.PistachiosServer

#! /bin/bash
base_dir=$(dirname $0)
 java -cp $base_dir/../lib/*:$base_dir/../config/ -Djava.library.path=$base_dir/../lib -Dlog4j.configuration=file://$base_dir/../config/log4j.xml com.yahoo.ads.pb.mttf.PistachiosMTTFTest

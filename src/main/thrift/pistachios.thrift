
service Pistachios {
   binary lookup(1: i64 partition, 2: i64 id),
   bool store(1: i64 partition, 2:i64 id, 3:binary value),
   bool process(1: i64 partition, 2:binary value),
   bool process_batch(1:binary value)
}


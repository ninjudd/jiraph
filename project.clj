(defproject jiraph "0.7.0"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [clojure-useful "0.3.8"]
                 [masai "0.5.1"]
                 [cereal "0.1.3"]
                 [retro "0.5.0"]]
  :dev-dependencies [[clojure-protobuf "0.4.5"]
                     [tokyocabinet "1.24.1-SNAPSHOT"]]
  :tasks [protobuf.tasks])

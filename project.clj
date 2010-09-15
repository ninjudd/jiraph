(defproject jiraph "0.1.0"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [tokyocabinet "1.23-SNAPSHOT"]
                 [clojure-useful "0.2.9-SNAPSHOT"]
                 [clojure-protobuf "0.2.10"]]
  :dev-dependencies [[clojure-protobuf "0.2.10"]]
  :tasks [protobuf.tasks])

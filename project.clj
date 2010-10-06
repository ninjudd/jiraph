(defproject jiraph "0.1.0"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0-beta1"]
                 [tokyocabinet "1.23-SNAPSHOT"]
                 [clojure-protobuf "0.2.4"]]
  :dev-dependencies [[clojure-protobuf "0.2.4"]]
  :tasks [protobuf.tasks])

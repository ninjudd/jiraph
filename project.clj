(defproject jiraph "0.6.0-SNAPSHOT"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [clojure-useful "0.3.1"]
                 [ruminate "0.5.0-SNAPSHOT"]
                 [retro "0.5.0-SNAPSHOT"]]
  :dev-dependencies [[clojure-protobuf "0.3.4-SNAPSHOT"]]
  :tasks [protobuf.tasks])

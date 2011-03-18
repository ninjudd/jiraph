(defproject jiraph "0.6.0"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [clojure-useful "0.3.2"]
                 [ruminate "0.5.0"]
                 [retro "0.5.0"]]
  :dev-dependencies [[clojure-protobuf "0.3.4"]]
  :tasks [protobuf.tasks])

(defproject jiraph "0.1.0"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [tokyocabinet "1.23-SNAPSHOT"]
                 [clojure-useful "0.3.0-SNAPSHOT"]
                 [clojure-protobuf "0.2.11-SNAPSHOT"]]
  :dev-dependencies [[clojure-protobuf "0.2.11-SNAPSHOT"]
                     [cake-autodoc "0.0.1-SNAPSHOT"]]
  :tasks [protobuf.tasks cake-autodoc.tasks])

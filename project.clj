(defproject jiraph "0.5.7-SNAPSHOT"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [tokyocabinet "1.23-SNAPSHOT"]
                 [clojure-useful "0.3.1"]]
  :dev-dependencies [[clojure-protobuf "0.3.4-SNAPSHOT"]
                     [cake-autodoc "0.0.1-SNAPSHOT"]]
  :tasks [protobuf.tasks cake-autodoc.tasks])

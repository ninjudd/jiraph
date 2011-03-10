(defproject jiraph "0.6.0-SNAPSHOT"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [tokyocabinet "1.23-SNAPSHOT"]
                 [clojure-useful "0.3.1"]
                 [retro "0.5.0-SNAPSHOT"]]
  :dev-dependencies [[clojure-protobuf "0.3.4-SNAPSHOT"]
                     [cake-autodoc "0.0.1-SNAPSHOT"]]
  :tasks [protobuf.tasks cake-autodoc.tasks])

(defproject jiraph "0.7.0-SNAPSHOT"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [useful "0.5.0"]
                 [masai "0.5.2-SNAPSHOT"]
                 [cereal "0.1.5-SNAPSHOT"]
                 [retro "0.5.0"]
                 [ego "0.1.1-SNAPSHOT"]]
  :dev-dependencies [[clojure-protobuf "0.4.7-SNAPSHOT"]
                     [tokyocabinet "1.24.1-SNAPSHOT"]]
  :tasks [protobuf.tasks])

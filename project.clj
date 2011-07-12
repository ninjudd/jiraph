(defproject jiraph "0.7.0-alpha2"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [useful "0.5.0"]
                 [masai "0.5.2"]
                 [cereal "0.1.5"]
                 [retro "0.5.0"]
                 [ego "0.1.1"]]
  :dev-dependencies [[protobuf "0.5.0-alpha1"]
                     [tokyocabinet "1.24.1-SNAPSHOT"]]
  :tasks [protobuf.tasks])

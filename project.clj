(defproject jiraph "0.7.0-beta10"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [useful "0.7.3"]
                 [masai "0.6.1"]
                 [cereal "0.1.10"]
                 [retro "0.5.2"]
                 [ego "0.1.7"]]
  :dev-dependencies [[protobuf "0.5.0-beta2"]
                     [tokyocabinet "1.24.1-SNAPSHOT" :ext true]]
  :cake-plugins [[cake-protobuf "0.5.0-beta1"]])

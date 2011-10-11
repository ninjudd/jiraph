(defproject jiraph "0.7.0-beta3"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [useful "0.7.0"]
                 [masai "0.5.3"]
                 [cereal "0.1.9"]
                 [retro "0.5.2"]
                 [ego "0.1.5"]]
  :dev-dependencies [[protobuf "0.5.0-beta1"]
                     [tokyocabinet "1.24.1-SNAPSHOT" :ext true]]
  :cake-plugins [[cake-protobuf "0.5.0-beta1"]])

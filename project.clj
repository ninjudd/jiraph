(defproject jiraph "0.7.0-beta7"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.2.0"]
                 [useful "0.7.5-alpha2"]
                 [masai "0.7.0-alpha4"]
                 [cereal "0.2.0-alpha2"]
                 [retro "0.6.0-alpha3"]
                 [io "0.1.0-alpha2"]
                 [ego "0.1.7"]]
  :dev-dependencies [[protobuf "0.5.0-beta1"]
                     [tokyocabinet "1.24.1-SNAPSHOT" :ext true]]
  :cake-plugins [[cake-protobuf "0.5.0-beta1"]])

(defproject jiraph "0.7.3"
  :description "embedded graph db library for clojure"
  :dependencies [[clojure "1.3.0"]
                 [useful "0.7.5"]
                 [masai "0.6.4"]
                 [cereal "0.1.11"]
                 [retro "0.5.4"]
                 [ego "0.1.7"]]
  :dev-dependencies [[protobuf "0.5.0"]
                     [tokyocabinet "1.24.1" :ext true]
                     [unk "0.9.3"]]
  :cake-plugins [[cake-protobuf "0.5.0"]])

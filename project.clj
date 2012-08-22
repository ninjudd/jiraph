(defproject jiraph "0.8.0"
  :description "embedded graph db library for clojure"
  :url "https://github.com/flatland/jiraph"
  :license {:name "Eclipse Public License - v 1.0"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [useful "0.8.3-alpha6"]
                 [masai "0.7.0-alpha9"]
                 [cereal "0.2.0-alpha3"]
                 [ordered "1.2.2"]
                 [schematic "0.0.6"]
                 [retro "0.7.0-alpha1"]
                 [io "0.2.0-beta2"]
                 [ego "0.1.7"]
                 [slingshot "0.10.3"]
                 [org.clojure/core.match "0.2.0-alpha9"]
                 [protobuf "0.6.1-beta3"]]
  :plugins [[lein-protobuf "0.2.0-beta7"]]
  :profiles {:1.5 {:dependencies [[org.clojure/clojure "1.5.0-master-SNAPSHOT"]]}
             :dev {:dependencies [[tokyocabinet "1.24.4"]
                                  [unk "0.9.3"]]}}
  :aliases {"testall" ["with-profile" "dev,default:dev,1.5,default" "test"]}
  :repositories {"sonatype-snapshots" {:url "http://oss.sonatype.org/content/repositories/snapshots"
                                       :snapshots true
                                       :releases {:checksum :fail :update :always}}})

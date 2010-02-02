Jiraph is an embedded graph database for clojure. It is extremely fast and can traverse
100,000 edges in about 6 seconds. It uses Tokyo Cabinet or Berkeley DB Java Edition for
backend storage.

## Installation

To download all necessary libraries automatically and install:
    ant package
    ant install

You can also specify options to configure (like the install prefix):
    ant package -Dconfigure='--prefix=/opt/local'
    ant install

Or you can specify custom locations for specific libraries:
    ant package -Dclojure.jar=$HOME/lib/clojure-1.1.0.jar -Dclojure.jar=$HOME/lib/clojure-contrib-1.1.0.jar \
                -Dtokyocabinet=$HOME/lib/tokyocabinet-1.4.42 -Djtokyocabinet=$HOME/lib/tokyocabinet-java-1.22

This code has been tested with clojure version 1.1.0 and tokyocabinet version 1.4.42.

## java.library.path

You must make sure the tokyocabinet libraries are in java.library.path for Jiraph to
work. You can do this by setting the LD_LIBRARY_PATH environment variable or using
'-Djava.library.path=/usr/local/lib' (which is the default install location).

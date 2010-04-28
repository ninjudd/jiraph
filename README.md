Jiraph is an embedded graph database for clojure. It is extremely fast and can walk
100,000 edges in about 3 seconds on my laptop. It uses Tokyo Cabinet for backend storage.

## Multi-layer Graph

For performance and scalability, graphs in Jiraph are multi-layer graphs. Nodes exist on
every layer. In this way, node data can be partitioned across all layers (similar to
column families in some nosql databases). For our purposes, we'll call the node data on a
particular layer a node slice. Edges, on the other hand, can only exist on a single
layer. All edges on a specific layer generally correspond in some way. The layer name can be
thought of as the edge type, or alternatively, multiple similar edge types can exist on
one layer.

Though layers can be used to organize your data, the primary motivation for layers is
performance. All data for each layer is stored in a separate Tokyo Cabinet DB, which
partitions the graph and speeds up walks by allowing them to load only the subset of the
graph data they need. This means you should strive to put all graph data needed for a
particular walk in one layer. This isn't always possible, but it will improve speed
because only one disk read will be required per walk step.

## Nodes and Edges

Every node slice is just a clojure map of attributes. It is conventional to use keywords
for the keys, but the values can be arbitrary clojure data structures. Each edge is also a
map of attributes. Internally, outgoing edges are actually stored as the `:edge` attribute
on the corresponding node slice. This way, a node and all its outgoing edges can be loaded
with one disk seek.

Nodes are not required to have a type, but it is conventional to include the node type in
its id if there are multiple types of nodes (e.g. "human:144567", "robot:23131"). The node
id is stored in the attributes map as `:id`.  Each edge also has a `:to-id` attribute
specifying which node it connects to. Here is an example node slice with outgoing edges:

  {:id        "human:1445677"
   :name      "Justin"
   :nicknames ["Judd" "Huck" "Judd Huck"]
   :edges     [{:to-id "robot:23131", :type :friend}
               {:to-id "human:1234",  :type :wife}
               {:to-id "dog:525152",  :type :pet}]
  }

## Performance

For faster performance, Jiraph supports using protocol buffers for node slices and edge data.

## Installation

To download all necessary libraries automatically and install:
    ant package
    ant install

You can also specify options to configure (like the install prefix):
    ant package -Dconfigure='--prefix=/opt/local'
    ant install

Or you can specify custom locations for specific libraries:
    ant package -Dclojure.jar=$HOME/lib/clojure-1.1.0.jar -Dclojure-contrib.jar=$HOME/lib/clojure-contrib-1.1.0.jar \
                -Dtokyocabinet=$HOME/lib/tokyocabinet-1.4.42 -Djtokyocabinet=$HOME/lib/tokyocabinet-java-1.22

This code has been tested with clojure version 1.1.0 and tokyocabinet version 1.4.42.

## java.library.path

You must make sure the tokyocabinet libraries are in java.library.path for Jiraph to
work. You can do this by setting the LD_LIBRARY_PATH environment variable or using
'-Djava.library.path=/usr/local/lib' (which is the default install location).

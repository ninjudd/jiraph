[![Build Status](https://secure.travis-ci.org/flatland/jiraph.png)](http://travis-ci.org/flatland/jiraph)

Jiraph is an embedded graph database for Clojure. It is extremely fast and can walk
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
performance. All data for each layer is stored in a separate data store, which partitions
the graph and speeds up walks by allowing them to load only the subset of the graph data
they need. This means you should strive to put all graph data needed for a particular walk
in one layer. This isn't always possible, but it will improve speed because only one disk
read will be required per walk step.

A Jiraph graph is just a clojure map of layer names (keywords) to datatypes that implement
the `jiraph.layer/Layer` protocol.

## Nodes and Edges

Every node slice is just a clojure map of attributes. It is conventional to use keywords
for the keys, but the values can be arbitrary clojure data structures. Each edge is also a
map of attributes. Internally, outgoing edges are stored as a map from node-ids to
attributes in the `:edges` attribute on the corresponding node slice. This way, a node and
all its outgoing edges can be loaded with one disk read.

Nodes are not required to have a type, but it is conventional to include the node type in
its id if there are multiple types of nodes (e.g. "human-144567", "robot-23131"). Here is
a sample node:

    {:name      "Justin"
     :nicknames ["Judd" "Huck" "Judd Huck"]
     :edges     {"human-2" {:type :spouse}
                 "robot-1" {:type :friend}
                 "dog-2"   {:type :pet}}
    }

## Usage

    (use 'jiraph.graph)

    (def g
      {:foo (jiraph.masai-layer/make "/tmp/foo")
       :bar (jiraph.masai-layer/make "/tmp/bar")
       :baz (jiraph.masai-layer/make "/tmp/baz")})

    (with-graph g
      (add-node! :foo "human-1" {:name "Justin"  :edges {"human-2" {:type :spouse}}})
      (add-node! :foo "human-2" {:name "Heather" :edges {"human-1" {:type :spouse}}})

      (get-node :foo "human-1"))
      ;; {:name "Justin", :edges {"human-2" {:type :spouse}}}

    (with-graph g
      (add-node! :foo "robot-1" {:name "Bender" :edges {"human-1" {:type :friend}}})
      (append-node! :foo "human-1" {:edges {"robot-1" {:type :friend}}})

      (get-node :foo "human-1"))
      ;; {:name "Justin", :edges {"robot-1" {:type :friend}, "human-2" {:type :spouse}}}

    (with-graph g
      (assoc-node! :foo "robot-1" {:designation "Bending Unit 22"})

      (get-node :foo "robot-1"))
      ;; {:edges {"human-1" {:type :friend}}, :name "Bender", :designation "Bending Unit 22"}

    (with-graph g
      (update-node! :foo "robot-1" dissoc :designation)

      (get-node :foo "robot-1"))
      ;; {:name "Bender", :edges {"human-1" {:type :friend}}}

## Revisions

You can use `at-revision` to mark changes with a given revision and rewind the state of
the graph back to that revision later.

    (with-graph g
      (at-revision 1
        (add-node! :foo "human-2" {:name "Ceruzzi"}))

      (at-revision 2
        (append-node! :foo "human-2" {:name "Hatcher"}))

      (:name (get-node :foo "human-2")) ;; "Hatcher"

      (at-revision 1
        (:name (get-node :foo "human-2"))) ;; "Ceruzzi"

      (:name (get-node :foo "human-2"))) ;; "Hatcher"


You can only use at-revision to rewind a layer's state if the layer was updated using only
add-node! and append-node!. All other update operations are destructive, and nodes modified
with update-node!, assoc-node! or delete-node! will not exist if you use at revision to go
back to before they were modified. To ensure that no destructive operations are permitted on
a layer, you can set :append-only in the metadata on the graph (either a set of layer
names that are append-only, or true for all layers). Even if a layer is marked append-only,
you can still call compact-node! to reduce the storage requirement and remove historical data.

Transactions also behave slightly different inside of at-revision. When with-transaction is
complete, it sets the :rev property on current layer to the current-revision. Also only the
first transaction on a layer for a given revision will be applied. Subsequent transactions
are assumed to be duplicates. This permits cross-layer transactions to be performed by
assigning the same revision number to all of them. Then if there is a failure in the middle
of a revision, the entire revision can be reapplied and layers that have already been updated
will be skipped.

## Performance

For faster performance, Jiraph supports using protocol buffers for node slices and edge data.

## Installation

The easiest way to use Jiraph in your project is via [Cake](http://github.com/ninjudd/cake).
Add the following to the `:dependencies` key in your `project.clj`:

    [jiraph "0.5.0-SNAPSHOT"]

Using Cake allows you to automatically pull in the native libraries for tokyocabinet and
compile protocol buffers if you are using them. Protocol Buffers, if used, should be
placed in your project's `proto/` directory and can be compiled by running `cake proto`.

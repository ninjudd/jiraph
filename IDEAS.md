- Add at least one (optional) key to each format
  - masai layer: just a :key-codec from [node-id]<=>db-key
    - we considered letting this just be a key, since the node-id is known, but this allows for easy reuse of more "general" codecs
  - sorted layer: two keys
    - :key-range for (start,stop) interval on wildcard paths
      - eg, ["profile-1:edges:" "profile-1:edges;"]
    - :key-codec from keyseq<=>db-key
      - eg, {"profile-1:edges:profile-2" => "profile-2"}









## New Plan
- Top-level record field, a codec for db-key<=>keyseq
  - We need it to figure out the id when doing stuff like node-seq
- sorted layer still needs a :range key in the format for wildcard paths

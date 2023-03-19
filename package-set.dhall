let upstream =
  https://github.com/dfinity/vessel-package-set/releases/download/mo-0.8.3-20230224/package-set.dhall

let packages = [
  { name = "stable-hash-map"
  , repo = "https://github.com/canscale/StableHashMap"
  , version = "v0.2.1"
  , dependencies = [ "base" ]
  },
  { name = "stable-rbtree"
  , repo = "https://github.com/canscale/StableRBTree"
  , version = "v0.6.0"
  , dependencies = [ "base" ]
  },
  { name = "btree"
  , repo = "https://github.com/canscale/StableHeapBTreeMap"
  , version = "v0.3.0"
  , dependencies = [ "base" ]
  },
  { name = "stable-buffer"
  , repo = "https://github.com/canscale/StableBuffer"
  , version = "v0.2.0"
  , dependencies = [ "base" ]
  },
  { name = "parser-combinators"
  , repo = "https://github.com/aviate-labs/parser-combinators.mo"
  , version = "v0.1.0"
  , dependencies = [ "base" ]
  },
  { name = "array"
  , repo = "https://github.com/aviate-labs/array.mo"
  , version = "v0.2.0"
  , dependencies = [ "base" ]
  },
  { name = "encoding"
  , repo = "https://github.com/aviate-labs/encoding.mo"
  , version = "v0.3.2"
  , dependencies = [ "array", "base" ]
  },
]

in  upstream # packages

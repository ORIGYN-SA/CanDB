let upstream =
  https://github.com/dfinity/vessel-package-set/releases/download/mo-0.6.21-20220215/package-set.dhall sha256:b46f30e811fe5085741be01e126629c2a55d4c3d6ebf49408fb3b4a98e37589b  

let packages = [
  { name = "stable-hash-map"
  , repo = "https://github.com/canscale/StableHashMap"
  , version = "v0.2.0"
  , dependencies = [ "base" ]
  },
  { name = "stable-rbtree"
  , repo = "https://github.com/canscale/StableRBTree"
  , version = "v0.3.0"
  , dependencies = [ "base" ]
  },
  { name = "parser-combinators"
  , repo = "https://github.com/aviate-labs/parser-combinators.mo"
  , version = "v0.1.0"
  , dependencies = [ "base" ]
  },
  { name = "json"
  , repo = "https://github.com/aviate-labs/json.mo"
  , version = "v0.1.1"
  , dependencies = [ "base", "parser-combinators" ]
  }
]

in  upstream # packages

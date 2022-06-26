import SHA256 "mo:crypto/SHA/SHA256";
import Debug "mo:base/Debug";
import Blob "mo:base/Blob";
import Text "mo:base/Text";
import Nat8 "mo:base/Nat8";

let sum = SHA256.sum(Blob.toArray(Text.encodeUtf8("hello world\n")));

Debug.print(debug_show(sum));

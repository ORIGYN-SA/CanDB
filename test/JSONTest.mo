import JSON "mo:json/JSON";
import Debug "mo:base/Debug";


let t = "{ \"name\":\"John\", \"age\":30, \"city\":\"New York\"}";

let p = JSON.Parser();

let res = p.parse(t);

switch(res) {
  case null {};
  case (?json) { Debug.print(JSON.show(json)) }
}



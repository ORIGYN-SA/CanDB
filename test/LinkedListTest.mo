import M "mo:matchers/Matchers";
import S "mo:matchers/Suite";
import T "mo:matchers/Testable";
import Nat "mo:base/Nat";
import LL "../src/LinkedList";
import LLM "./LinkedListMatchers";

let { run;test;suite; } = S;

let emptySuite = suite("empty", 
  [
    test("is null with correct type",
      LL.empty<Nat>(),
      M.equals(LLM.testableLinkedList<Nat>(null, Nat.toText, Nat.equal))
    )
  ]
);

let prependSuite = suite("prepend",
  [
    test("if the list is null, creates a list with the prepended element",
      LL.prepend<Nat>(5, LL.empty<Nat>()),
      M.equals(LLM.testableLinkedList<Nat>(?{
        var head = {
          value = 5;
          var next = null;
        };
        var tail = {
          value = 5;
          var next = null;
        };
      }, Nat.toText, Nat.equal))
    ),
    test("prepends an element to the head existing list",
      do {
        let el2: LL.LinkedListElement<Nat> = {
          value = 10;
          var next = null;
        };
        let el1: LL.LinkedListElement<Nat> = {
          value = 8;
          var next = ?el2;
        };
        LL.prepend<Nat>(5, ?{
          var head = el1;
          var tail = el2;
        })
      },
      M.equals(LLM.testableLinkedList<Nat>(?{
        var head = {
          value = 5;
          var next = ?{
            value = 8;
            var next = ?{
              value = 10;
              var next = null;
            }
          }
        };
        var tail = {
          value = 10;
          var next = null;
        }
      }, Nat.toText, Nat.equal))
    )
  ]
);

let appendSuite = suite("append",
  [
    test("if the list is null, creates a list with the appended element",
      LL.append<Nat>(LL.empty<Nat>(), 5),
      M.equals(LLM.testableLinkedList<Nat>(?{
        var head = {
          value = 5;
          var next = null;
        };
        var tail = {
          value = 5;
          var next = null;
        };
      }, Nat.toText, Nat.equal))
    ),
    test("appends an element to the tail of an existing list",
      do {
        let el2: LL.LinkedListElement<Nat> = {
          value = 10;
          var next = null;
        };
        let el1: LL.LinkedListElement<Nat> = {
          value = 8;
          var next = ?{
            value = 9;
            var next = ?el2;
          }
        };
        LL.append<Nat>(?{
          var head = el1;
          var tail = el2;
        }, 5)
      },
      M.equals(LLM.testableLinkedList<Nat>(?{
        var head = {
          value = 8;
          var next = ?{
            value = 9;
            var next = ?{
              value = 10;
              var next = ?{
                value = 5;
                var next = null;
              }
            }
          }
        };
        var tail = {
          value = 5;
          var next = null;
        }
      }, Nat.toText, Nat.equal))
    )
  ]
);

let mergeSuite = suite("merge",
  [
    test("merging two null lists returns the null list",
      LL.merge<Nat>(LL.empty<Nat>(), LL.empty<Nat>()),
      M.equals(LLM.testableLinkedList(LL.empty<Nat>(), Nat.toText, Nat.equal))
    ),
    test("merging a null list with a populated list returns the populated list",
      LL.merge<Nat>(
        LL.empty<Nat>(), 
        ?{ 
          var head = {
            value = 5;
            var next = null;
          };
          var tail = {
            value = 5;
            var next = null;
          }
        }
      ), 
      M.equals(LLM.testableLinkedList<Nat>(
        ?{
          var head = {
            value = 5;
            var next = null;
          };
          var tail = {
            value = 5;
            var next = null;
          }
        },
        Nat.toText,
        Nat.equal
      ))
    ),
    test("merging a populated list a null list returns the populated list",
      LL.merge<Nat>(
        ?{ 
          var head = {
            value = 5;
            var next = null;
          };
          var tail = {
            value = 5;
            var next = null;
          }
        },
        LL.empty<Nat>()
      ), 
      M.equals(LLM.testableLinkedList<Nat>(
        ?{
          var head = {
            value = 5;
            var next = null;
          };
          var tail = {
            value = 5;
            var next = null;
          }
        },
        Nat.toText,
        Nat.equal
      ),
    )),
    test("merging two lists, l1 and l2, returns them in the order provided with l1.head at the head and l2.tail at the tail",
      do {
        let l1tailElement: LL.LinkedListElement<Nat> = {
          value = 8;
          var next = null;
        };
        let l2headtailElement: LL.LinkedListElement<Nat> = {
          value = 7;
          var next = null;
        };
        LL.merge<Nat>(
          ?{ 
            var head = {
              value = 5;
              var next = ?l1tailElement;
            };
            var tail = l1tailElement; 
          },
          ?{ 
            var head = l2headtailElement;
            var tail = l2headtailElement; 
          }
        )
      },
      M.equals(LLM.testableLinkedList<Nat>(
        ?{
          var head = {
            value = 5;
            var next = ?{
              value = 8;
              var next = ?{
                value = 7;
                var next = null;
              };
            };
          };
          var tail = {
            value = 7;
            var next = null;
          }
        },
        Nat.toText,
        Nat.equal
      ))
    )
  ]
);

run(suite("LinkedList",
  [
    emptySuite,
    prependSuite,
    appendSuite,
    mergeSuite
  ]
))
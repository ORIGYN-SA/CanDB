import Bool "mo:base/Bool";
import Text "mo:base/Text";

/// A Mutable Linked List
module {
  public type LinkedListElement<T> = {
    value: T;
    var next: ?LinkedListElement<T>;
  };

  public type LinkedList<T> = ?{
    var head: LinkedListElement<T>;
    var tail: LinkedListElement<T>;
  };

  public func empty<T>(): LinkedList<T> { null };

  public func prepend<T>(x: T, l: LinkedList<T>): LinkedList<T> {
    switch(l) {
      case null { 
        var element: LinkedListElement<T> = { value = x; var next = null };
        ?{
          var head = element;
          var tail = element;
        }
      };
      case (?list) {
        var element: LinkedListElement<T> = { value = x; var next = ?list.head };
        list.head := element;
        ?list
      }
    }
  };

  public func append<T>(l: LinkedList<T>, x: T): LinkedList<T> {
    switch(l) {
      case null { 
        var element: LinkedListElement<T> = { value = x; var next = null };
        ?{
          var head = element;
          var tail = element;
        }
      };
      case (?list) {
        var element: LinkedListElement<T> = { value = x; var next = null };
        list.tail.next := ?element;
        list.tail := element;
        ?list
      }
    }
  };

  public func merge<T>(l1: LinkedList<T>, l2: LinkedList<T>): LinkedList<T> {
    switch(l1, l2) {
      case (null, null) { null };
      case (?l1, null) { ?l1 };
      case (null, ?l2) { ?l2 };
      case (?l1, ?l2) {
        l1.tail.next := ?l2.head;
        l1.tail := l2.tail;
        ?l1
      }
    }
  };

  public func equal<T>(l1: LinkedList<T>, l2: LinkedList<T>, equal: (T, T) -> Bool): Bool {
    switch(l1, l2) {
      case (null, null) { true };
      case (?l1, ?l2) {
        func linkedListElementsEqual(e1: ?LinkedListElement<T>, e2: ?LinkedListElement<T>): Bool {
          switch(e1, e2) {
            case (null, null) { true };
            case (?e1, ?e2) {
              if (equal(e1.value, e2.value)) {
                linkedListElementsEqual(e1.next, e2.next)
              } else {
                false
              }
            };
            case _ { false }
          }
        };

        linkedListElementsEqual(?l1.head, ?l2.head) and linkedListElementsEqual(?l1.tail, ?l2.tail)
      };
      case _ { false }
    }
  };

  public func toText<T>(ll: LinkedList<T>, toText: T -> Text): Text {
    switch(ll) {
      case null { "null" };
      case (?ll) {
        func elementsToText(el: ?LinkedListElement<T>): Text {
          switch(el) {
            case null { "null "};
            case (?el) { "{ value=" # toText(el.value) # ", next=" # elementsToText(el.next) # " }" };
          }
        };

        elementsToText(?ll.head)
      }
    }
  }
}

import T "mo:matchers/Testable";
import LL "../src/LinkedList";
import Bool "mo:base/Bool";
import Text "mo:base/Text";

module {
  public func testableLinkedList<T>(ll: LL.LinkedList<T>, toText: T -> Text, equal: (T, T) -> Bool): T.TestableItem<LL.LinkedList<T>> = {
    display = func(ll: LL.LinkedList<T>): Text = LL.toText(ll, toText);
    equals = func(l1: LL.LinkedList<T>, l2: LL.LinkedList<T>): Bool = LL.equal<T>(l1, l2, equal);
    item = ll;
  };
}
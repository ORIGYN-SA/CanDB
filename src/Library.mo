/// A shiny new library
///
/// Make it easy and fun to use your new library by including some module specific documentation here.
/// It's always a good idea to include a minimal working example:
///
/// ```motoko
/// import LibraryTemplate "mo:library-template/Library";
///
/// assert(LibraryTemplate.isPalindrome("anna"));
/// assert(not LibraryTemplate.isPalindrome("christoph"));
/// ```

import Array "mo:base/Array";
import Iter "mo:base/Iter";
import Text "mo:base/Text";

module {
  // I'm a private function that's not exposed to consumers of this library
  func reverseText(t : Text) : Text {
    let chars : [Char] = Iter.toArray(Text.toIter(t));
    let size = chars.size();
    let reversedChars = Array.tabulate(size, func (i : Nat) : Char {
      chars[size - i - 1]
    });
    Text.fromIter(Iter.fromArray(reversedChars));
  };

  /// Checks whether the given input text is equal to itself when reversed.
  public func isPalindrome(input : Text) : Bool {
    let reversed = reverseText(input);
    input == reversed
  };
}

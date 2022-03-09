import M "mo:matchers/Matchers";
import Library "../src/Library";
import S "mo:matchers/Suite";
import T "mo:matchers/Testable";

let suite = S.suite("isPalindrome", [
    S.test("anna is a palindrome",
      Library.isPalindrome("anna"),
      M.equals(T.bool(true))),
    S.test("christoph is not a palindrome",
      Library.isPalindrome("christoph"),
      M.equals(T.bool(false))),
]);

S.run(suite);

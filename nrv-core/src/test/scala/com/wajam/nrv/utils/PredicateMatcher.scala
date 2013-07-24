package com.wajam.nrv.utils

import org.mockito.ArgumentMatcher

/**
 * Allow the use of a predicate as a matcher when using Mockito.
 *
 * It is more scala friendly than creating a new class for each matchers.
 *
 * Ex:
 *
 * val test = "ABC"
 *
 * mock.foo(test)
 *
 * verify(mock).foo(argThat(new PredicateMatcher((s: String) =>  test == s)))
 *
 * @param predicate The predicate
 * @tparam T The type of the argument of the predicate and of argument to verify
 */
class PredicateMatcher[T](predicate: (T) => Boolean) extends ArgumentMatcher {

  def matches(ref: Object): Boolean = {
    ref match {
      case refT: T => predicate(refT)
      case unknownRefT =>
        throw new IllegalArgumentException(
          ("The predicate doesn't provide the right parameter for that match." +
            " The required type is: %s").format(unknownRefT.getClass.getCanonicalName))
    }
  }
}
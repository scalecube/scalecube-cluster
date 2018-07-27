package io.scalecube;

/**
 * Static utility methods pertaining to {@code String} or {@code CharSequence} instances.
 */
public final class Strings {
  private Strings() {}

  /**
   * Returns {@code true} if the given string is null or is the empty string.
   *
   * @param string a string reference to check
   * @return {@code true} if the string is null or is the empty string
   */
  public static boolean isNullOrEmpty(String string) {
    return string == null || string.length() == 0;
  }
}

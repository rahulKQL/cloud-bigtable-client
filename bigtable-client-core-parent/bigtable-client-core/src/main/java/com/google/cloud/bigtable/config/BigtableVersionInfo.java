package com.google.cloud.bigtable.config;

import com.google.common.base.Strings;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.regex.Pattern;

public class BigtableVersionInfo {

  private static final Logger LOG = new Logger(BigtableVersionInfo.class);

  public static final String CLIENT_VERSION = getVersion();
  public static final String JDK_VERSION = getJavaVersion();

  public static final String CORE_USER_AGENT = "bigtable-" + CLIENT_VERSION + ",jdk-" + JDK_VERSION;

  /**
   * Gets user agent from bigtable-version.properties. Returns a default dev user agent with current
   * timestamp if not found.
   */
  private static String getVersion() {
    final String defaultVersion = "dev-" + System.currentTimeMillis();
    final String fileName = "bigtable-version.properties";
    final String versionProperty = "bigtable.version";
    try (InputStream stream = BigtableVersionInfo.class.getResourceAsStream(fileName)) {
      if (stream == null) {
        LOG.error("Could not load properties file bigtable-version.properties");
        return defaultVersion;
      }

      Properties properties = new Properties();
      properties.load(stream);
      String value = properties.getProperty(versionProperty);
      if (value == null) {
        LOG.error("%s not found in %s.", versionProperty, fileName);
      } else if (value.startsWith("$")) {
        LOG.info("%s property is not replaced.", versionProperty);
      } else {
        return value;
      }
    } catch (IOException e) {
      LOG.error("Error while trying to get user agent name from %s", e, fileName);
    }
    return defaultVersion;
  }

  /** @return The java specification version; for example, 1.7 or 1.8. */
  private static String getJavaVersion() {
    return System.getProperty("java.specification.version");
  }

  public static void isVersionStable() {
    final String currentVersion = getVersion();
    if (currentVersion.matches("(\\d|.)+" + "-SNAPSHOT\\b")
        || currentVersion.matches("\\bdev-\\d+")) {
      LOG.warn("%s is development version", currentVersion);
      return;
    }

    final String stableVersion = "stable.versions";
    final String obsoleteVersion = "obsolete.versions";
    final String fileName = "bigtable-version.properties";
    final Pattern pattern = Pattern.compile("(\\b|,)" + currentVersion + "(\\b|,)");
    try (InputStream stream = BigtableVersionInfo.class.getResourceAsStream(fileName)) {
      if (stream == null) {
        LOG.error("Could not load properties file %s", fileName);
        return;
      }

      Properties properties = new Properties();
      properties.load(stream);
      String value = properties.getProperty(stableVersion);
      if (Strings.isNullOrEmpty(value)) {
        LOG.error("%s not found in %s.", currentVersion, stableVersion);
      } else if (pattern.matcher(value).find()) {
        LOG.info("%s is a stable bigtable-client.version", currentVersion);
        return;
      }

      String obsolete = properties.getProperty(obsoleteVersion);
      if (Strings.isNullOrEmpty(obsolete)) {
        LOG.error("%s not found in %s.", obsoleteVersion, fileName);
      } else if (pattern.matcher(obsolete).find()) {
        LOG.error(
            "%s is an obsolete version, Please upgrade your bigtable-client-version.",
            currentVersion);
        return;
      }

      LOG.error("%s is an unknown version, Please verify before moving forward", currentVersion);
    } catch (IOException e) {
      LOG.error("Error while trying to get version from %s", e, fileName);
    }
  }
}

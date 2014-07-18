/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
<<<<<<< HEAD
// Copyright (C) 2010-2012 Pentaho
=======
// Copyright (C) 2010-2013 Pentaho
>>>>>>> upstream/4.0
// All Rights Reserved.
*/
package mondrian.server;

import mondrian.olap.MondrianServer;
import mondrian.olap.Util;
import mondrian.spi.CatalogLocator;
import mondrian.spi.impl.IdentityCatalogLocator;
import mondrian.util.LockBox;

<<<<<<< HEAD
import java.io.*;
import java.net.URL;
import java.util.*;
=======
import org.apache.log4j.Logger;
>>>>>>> upstream/4.0

/**
 * Registry of all servers within this JVM, and also serves as a factory for
 * servers.
 *
 * <p>This class is not a public API. User applications should use the
 * methods in {@link mondrian.olap.MondrianServer}.
 *
 * @author jhyde
 */
public class MondrianServerRegistry {
    public static final Logger logger =
        Logger.getLogger(MondrianServerRegistry.class);
    public static final MondrianServerRegistry INSTANCE =
        new MondrianServerRegistry();

    public MondrianServerRegistry() {
        super();
    }

    /**
     * Registry of all servers.
     */
    final LockBox lockBox = new LockBox();

    /**
     * The one and only one server that does not have a repository.
     */
    final MondrianServer staticServer =
        createWithRepository(null, null);

    /**
     * Looks up a server with a given id. If the id is null, returns the
     * static server.
     *
     * @param instanceId Unique identifier of server instance
     * @return Server
     * @throws RuntimeException if no server instance exists
     */
    public MondrianServer serverForId(String instanceId) {
        if (instanceId != null) {
            final LockBox.Entry entry = lockBox.get(instanceId);
            if (entry == null) {
                throw Util.newError(
                    "No server instance has id '" + instanceId + "'");
            }
            return (MondrianServer) entry.getValue();
        } else {
            return staticServer;
        }
    }

    public MondrianServer.MondrianVersion getVersion() {
        if (logger.isDebugEnabled()) {
            logger.debug(" Vendor: " + MondrianServerVersion.VENDOR);
            final String title = MondrianServerVersion.NAME;
            logger.debug("  Title: " + title);
            final String versionString = MondrianServerVersion.VERSION;
            logger.debug("Version: " + versionString);
            final int majorVersion = MondrianServerVersion.MAJOR_VERSION;
            logger.debug(String.format("Major Version: %d", majorVersion));
            final int minorVersion = MondrianServerVersion.MINOR_VERSION;
            logger.debug(String.format("Minor Version: %d", minorVersion));
        }
<<<<<<< HEAD
        return version;
    }

    private static String[] loadVersionFile() {
        // First, try to read the version info from the package. If the classes
        // came from a jar, this info will be set from manifest.mf.
        Package pakkage = MondrianServerImpl.class.getPackage();
        String implementationVersion = pakkage.getImplementationVersion();

        // Second, try to read VERSION.txt.
        String version = "Unknown Version";
        String title = "Unknown Database";
        String vendor = "Unknown Vendor";
        URL resource =
            MondrianServerImpl.class.getClassLoader()
                .getResource("DefaultRules.xml");
        if (resource != null) {
            try {
                String path = resource.getPath();
                String path2 =
                    Util.replace(
                        path, "DefaultRules.xml", "VERSION.txt");
                URL resource2 =
                    new URL(
                        resource.getProtocol(),
                        resource.getHost(),
                        path2);

                // Parse VERSION.txt. E.g.
                //   Title: mondrian
                //   Version: 3.4.9
                // becomes {("Title", "mondrian"), ("Version", "3.4.9")}
                final Map<String, String> map = new HashMap<String, String>();
                final LineNumberReader r =
                    new LineNumberReader(
                        new InputStreamReader(resource2.openStream()));
                try {
                    String line;
                    while ((line = r.readLine()) != null) {
                        int i = line.indexOf(": ");
                        if (i >= 0) {
                            String key = line.substring(0, i);
                            String value = line.substring(i + ": ".length());
                            map.put(key, value);
                        }
                    }
                } finally {
                    r.close();
                }

                title = map.get("Title");
                version = map.get("Version");
                try {
                    Integer.parseInt(version);
                } catch (NumberFormatException e) {
                    // Version is not a number (e.g. "TRUNK-SNAPSHOT").
                    // Fall back on VersionMajor, VersionMinor, if present.
                    String versionMajor = map.get("VersionMajor");
                    String versionMinor = map.get("VersionMinor");
                    if (versionMajor != null) {
                        version = versionMajor;
                    }
                    if (versionMinor != null) {
                        version += "." + versionMinor;
                    }
                }
                vendor = map.get("Vendor");
            } catch (IOException e) {
                // ignore exception - it's OK if file is not found
                Util.discard(e);
=======
        final StringBuilder sb = new StringBuilder();
        try {
            Integer.parseInt(MondrianServerVersion.VERSION);
            sb.append(MondrianServerVersion.VERSION);
        } catch (NumberFormatException e) {
            // Version is not a number (e.g. "TRUNK-SNAPSHOT").
            // Fall back on VersionMajor, VersionMinor, if present.
            final String versionMajor =
                String.valueOf(MondrianServerVersion.MAJOR_VERSION);
            final String versionMinor =
                String.valueOf(MondrianServerVersion.MINOR_VERSION);
            if (versionMajor != null) {
                sb.append(versionMajor);
            }
            if (versionMinor != null) {
                sb.append(".").append(versionMinor);
>>>>>>> upstream/4.0
            }
        }
        return new MondrianServer.MondrianVersion() {
            public String getVersionString() {
                return sb.toString();
            }
            public String getProductName() {
                return MondrianServerVersion.NAME;
            }
            public int getMinorVersion() {
                return MondrianServerVersion.MINOR_VERSION;
            }
            public int getMajorVersion() {
                return MondrianServerVersion.MAJOR_VERSION;
            }
        };
    }

    public MondrianServer createWithRepository(
        RepositoryContentFinder contentFinder,
        CatalogLocator catalogLocator)
    {
        if (catalogLocator == null) {
            catalogLocator = new IdentityCatalogLocator();
        }
        final Repository repository;
        if (contentFinder == null) {
            // NOTE: registry.staticServer is initialized by calling this
            // method; this is the only time that it is null.
            if (staticServer != null) {
                return staticServer;
            }
            repository = new ImplicitRepository();
        } else {
            repository = new FileRepository(contentFinder, catalogLocator);
        }
        return new MondrianServerImpl(this, repository, catalogLocator);
    }
}

// End MondrianServerRegistry.java

/*
// This software is subject to the terms of the Eclipse Public License v1.0
// Agreement, available at the following URL:
// http://www.eclipse.org/legal/epl-v10.html.
// You must accept the terms of that agreement to use this software.
//
<<<<<<< HEAD
// Copyright (C) 2012-2012 Pentaho
=======
// Copyright (C) 2012-2013 Pentaho
>>>>>>> upstream/4.0
// All Rights Reserved.
*/
package mondrian.rolap;

import mondrian.olap.Util;
import mondrian.olap.Util.PropertyList;
<<<<<<< HEAD
import mondrian.spi.DynamicSchemaProcessor;
=======
import mondrian.spi.*;
>>>>>>> upstream/4.0
import mondrian.test.FoodMartTestCase;
import mondrian.test.TestContext;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import javax.sql.DataSource;

/**
 * Test for {@link RolapSchemaPool}.
 */
public class RolapSchemaPoolTest extends FoodMartTestCase {

    public RolapSchemaPoolTest(String name) {
        super(name);
    }

    public void testBasicSchemaFetch() {
        RolapSchemaPool schemaPool = RolapSchemaPool.instance();
        schemaPool.clear();

        String catalogUrl = getFoodmartCatalogUrl().toString();
        Util.PropertyList connectInfo =
            Util.parseConnectString(TestContext.getDefaultConnectString());

        RolapSchema schema =
            schemaPool.get(
                catalogUrl,
                "connectionKeyA",
                "joeTheUser",
                "aDataSource",
                connectInfo);
        RolapSchema schemaA =
            schemaPool.get(
                catalogUrl,
                "connectionKeyA",
                "joeTheUser",
                "aDataSource",
                connectInfo);
<<<<<<< HEAD
        //same arguments, same object
=======
        // same arguments, same object
>>>>>>> upstream/4.0
        assertTrue(schema == schemaA);
    }

    public void testSchemaFetchCatalogUrlJdbcUuid() {
        RolapSchemaPool schemaPool = RolapSchemaPool.instance();
        schemaPool.clear();
        final String uuid = "UUID-1";

        String catalogUrl = getFoodmartCatalogUrl().toString();
        Util.PropertyList connectInfo =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        connectInfo.put(
            RolapConnectionProperties.JdbcConnectionUuid.name(),
            uuid);

        // Put in pool
        RolapSchema schema =
            schemaPool.get(
                catalogUrl,
                "connectionKeyA",
                "joeTheUser",
                "aDataSource",
                connectInfo);

        // Same catalogUrl, same JdbcUuid
        Util.PropertyList connectInfoA =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        connectInfoA.put(
            RolapConnectionProperties.JdbcConnectionUuid.name(),
            uuid);
        RolapSchema sameSchema =
            schemaPool.get(
                catalogUrl,
                "aDifferentConnectionKey",
                "mrDoeTheOtherUser",
                "someDataSource",
                connectInfoA);
<<<<<<< HEAD
        //must fetch the same object
=======
        // must fetch the same object
>>>>>>> upstream/4.0
        assertTrue(schema == sameSchema);

        connectInfo.put(
            RolapConnectionProperties.JdbcConnectionUuid.name(),
            "SomethingCompletelyDifferent");
        RolapSchema aNewSchema =
            schemaPool.get(
                catalogUrl,
                "connectionKeyA",
                "joeTheUser",
                "aDataSource",
                connectInfo);
<<<<<<< HEAD
        //must create a new object
=======
        // must create a new object
>>>>>>> upstream/4.0
        assertTrue(schema != aNewSchema);
    }

    /**
     * Test using JdbcConnectionUUID and useSchemaChecksum
     * fetches the same schema in all scenarios.
     */
    public void testSchemaFetchMd5JdbcUid() throws IOException {
        RolapSchemaPool pool = RolapSchemaPool.instance();
        pool.clear();
        final String uuid = "UUID-1";
        String catalogUrl = getFoodmartCatalogUrl().toString();
        Util.PropertyList connectInfo =
            Util.parseConnectString(TestContext.getDefaultConnectString());
        connectInfo.put(
            RolapConnectionProperties.JdbcConnectionUuid.name(),
            uuid);
        connectInfo.put(
            RolapConnectionProperties.UseContentChecksum.name(),
            "true");

        RolapSchema schema =
            pool.get(
                catalogUrl,
                "connectionKeyA",
                "joeTheUser",
                "aDataSource",
                connectInfo);

        Util.PropertyList connectInfoDyn = connectInfo.clone();
        connectInfoDyn.put(
            RolapConnectionProperties.DynamicSchemaProcessor.name(),
            NotReallyDynamicSchemaProcessor.class.getName());
        RolapSchema schemaDyn =
            pool.get(
                catalogUrl,
                "connectionKeyB",
                "jed",
                "dsName",
                connectInfo);

        assertTrue(schema == schemaDyn);

        String catalogContent = Util.readVirtualFileAsString(catalogUrl);
        Util.PropertyList connectInfoCont = connectInfo.clone();
        connectInfoCont.remove(RolapConnectionProperties.Catalog.name());
        connectInfoCont.put(
            RolapConnectionProperties.CatalogContent.name(),
            catalogContent);
        RolapSchema schemaCont = pool.get(
            catalogUrl,
            "connectionKeyC", "--", "--", connectInfo);

        assertTrue(schema == schemaCont);

        Util.PropertyList connectInfoDS = connectInfo.clone();
        final StringBuilder buf = new StringBuilder();
<<<<<<< HEAD
        DataSource dataSource =
            RolapConnection.createDataSource(null, connectInfoDS, buf);
=======
        DataServicesProvider provider =
            DataServicesLocator.getDataServicesProvider("");
        DataSource dataSource =
            provider.createDataSource(null, connectInfoDS, buf);
>>>>>>> upstream/4.0
        RolapSchema schemaDS = pool.get(catalogUrl, dataSource, connectInfoDS);

        assertTrue(schema == schemaDS);
    }


    protected URL getFoodmartCatalogUrl() {
        // Works if we are running in root directory of source tree
<<<<<<< HEAD
        File file = new File("demo/FoodMart.xml");
        if (!file.exists()) {
            // Works if we are running in bin directory of runtime env
            file = new File("../demo/FoodMart.xml");
=======
        File file = new File("demo/FoodMart.mondrian.xml");
        if (!file.exists()) {
            // Works if we are running in bin directory of runtime env
            file = new File("../demo/FoodMart.mondrian.xml");
>>>>>>> upstream/4.0
        }
        try {
            return Util.toURL(file);
        } catch (MalformedURLException e) {
            throw new Error(e.getMessage());
        }
    }

    public static class NotReallyDynamicSchemaProcessor
        implements DynamicSchemaProcessor
    {
        public String processSchema(String schemaUrl, PropertyList connectInfo)
            throws Exception
        {
            return Util.readVirtualFileAsString(schemaUrl);
        }
    }
<<<<<<< HEAD
}
=======

}

>>>>>>> upstream/4.0
// End RolapSchemaPoolTest.java

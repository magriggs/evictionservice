package com.gridgain.proserv;

import com.gridgain.proserv.evictionservice.model.Declaration;
import com.gridgain.proserv.evictionservice.model.DeclarationMaster;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;

public class ServerNode {
    public static void main(String[] args) {
        boolean loadFromH2Database = false;

        // run with argument "--db" to create/populate a local H2 database
        if (args.length > 0 && args[0].toLowerCase().equals("--db"))
            loadFromH2Database = true;

        new ServerNode().run(loadFromH2Database);
    }

    private void run(boolean loadFromH2Database) {

        try (Ignite ignite = Ignition.start("ignite-server.xml")) {

            // this should only be called once the cluster topology is complete
            ignite.active(true);

            IgniteCache<Integer, DeclarationMaster> masterCache = ignite.getOrCreateCache("DeclarationMasterCache");

            IgniteCache<Integer, Declaration> declarationCache = ignite.getOrCreateCache("DeclarationCache");

            try (Connection conn = getConnection()) {
                createTables(conn);
                if (loadFromH2Database)
                    populateDatabaseAndCache(ignite, conn, masterCache, declarationCache);
            }
            catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                while (true)
                    Thread.sleep(100);

            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void populateDatabaseAndCache(Ignite ignite, Connection conn,
        IgniteCache<Integer, DeclarationMaster> decMasterCache,
        IgniteCache<Integer, Declaration> decsCache) {

        Random rng = new Random();
        String declarationMasterSql = "INSERT INTO DECLARATION_MASTER(DECLARATION_ID) VALUES(%d)";
        String declarationRowSql = "INSERT INTO DECLARATIONS(ROW_ID, DECLARATION_ID, VALUE) VALUES(%d,%d,%f)";

        try (IgniteDataStreamer<Integer, Declaration> ds = ignite.dataStreamer(decsCache.getName())) {
            ds.allowOverwrite(true);
            Statement stmt = conn.createStatement();

            System.out.println("Truncate database tables");
            stmt.execute("TRUNCATE TABLE DECLARATION_MASTER");
            stmt.execute("TRUNCATE TABLE DECLARATIONS");

            System.out.println("Clear Ignite caches");
            decMasterCache.clear();
            decsCache.clear();

            // populate database and cache independently, as customer does not use Ignite CacheStore implementation
            int decRow = 0;
            for (int decId = 0; decId < 100; decId++) {
                stmt.execute(String.format(declarationMasterSql, decId));
                decMasterCache.put(decId, new DeclarationMaster(decId));

                int numberOfDeclarations = 500 + rng.nextInt(5000);
                for (int j = 0; j < numberOfDeclarations; j++) {
                    float value = Math.abs(10_000 * rng.nextFloat());
                    ds.addData(decRow++, new Declaration(decId, decRow, value));
                    stmt.execute(String.format(declarationRowSql, decRow, decId, value));
                }

                if (decId % 10 == 0)
                    System.out.println("Created and cached " + decId + " declarations");
            }
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("Database loaded");
    }

    private void createTables(Connection conn) {
        try {
            PreparedStatement st = conn.prepareStatement("CREATE TABLE IF NOT EXISTS DECLARATION_MASTER(DECLARATION_ID int)"); // TRUNCATE TABLE DECLARATION_MASTER;");
            st.execute();
            st = conn.prepareStatement("CREATE TABLE IF NOT EXISTS DECLARATIONS(ROW_ID int, DECLARATION_ID int, VALUE float)");// TRUNCATE TABLE DECLARATIONS;");
            st.execute();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }

    // start an H2 database server from the command line
    private Connection getConnection() {
        try {
            Class.forName("org.h2.Driver");

            Connection conn = DriverManager.getConnection("jdbc:h2:tcp://localhost/EvictionExampleDatabase", "sa", "");
            return conn;
        }
        catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }

        return null;
    }
}

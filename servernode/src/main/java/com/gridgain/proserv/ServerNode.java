package com.gridgain.proserv;

import com.gridgain.proserv.evictionservice.model.Declaration;
import com.gridgain.proserv.evictionservice.model.DeclarationMaster;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;

public class ServerNode {
    public static void main(String[] args) {
        boolean createData = false;

        // run with "--clear" argument to clear old data and generate new.  If no argument is supplied, previous data will be loaded from Ignite Persistent Store
        if (args.length > 0 && args[0].toLowerCase().equals("--clear"))
            createData = true;
        else
            System.out.printf("Loading data from Ignite Persistent Store...");

        new ServerNode().run(createData);
    }

    private void run(boolean createData) {

        try (Ignite ignite = Ignition.start("ignite-server.xml")) {

            // this should only be called once the cluster topology is complete
            ignite.active(true);

            IgniteCache<Integer, DeclarationMaster> masterCache = ignite.getOrCreateCache("DeclarationMasterCache");

            IgniteCache<Integer, Declaration> declarationCache = ignite.getOrCreateCache("DeclarationCache");

            if (createData)
                createDataAndCache(ignite, masterCache, declarationCache);

            try {
                while (true)
                    Thread.sleep(100);

            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void createDataAndCache(Ignite ignite,
        IgniteCache<Integer, DeclarationMaster> decMasterCache,
        IgniteCache<Integer, Declaration> decsCache) {

        System.out.println("Generating data...");
        try (IgniteDataStreamer<Integer, Declaration> ds = ignite.dataStreamer(decsCache.getName())) {
            ds.allowOverwrite(true);

            System.out.println("Clear Ignite caches");
            decMasterCache.clear();
            decsCache.clear();

            // populate database and cache independently, as customer does not use Ignite CacheStore implementation
            int decRow = 0;
            Random rng = new Random();

            for (int decId = 0; decId < 100; decId++) {
                decMasterCache.put(decId, new DeclarationMaster(decId));

                int numberOfDeclarations = 500 + rng.nextInt(500);
                for (int j = 0; j < numberOfDeclarations; j++) {
                    float value = Math.abs(10_000 * rng.nextFloat());
                    ds.addData(decRow++, new Declaration(decId, decRow, value));
                }

                if (decId % 10 == 0)
                    System.out.println("Created and cached " + decId + " declarations");
            }
        }
    }
}

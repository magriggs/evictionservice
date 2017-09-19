import com.gridgain.proserv.evictionservice.model.Declaration;
import com.gridgain.proserv.evictionservice.model.DeclarationMaster;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteFutureTimeoutException;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.stream.StreamReceiver;

public class EvictionClient {

    public static void main(String[] args) {
        if (args == null || args.length < 1) {
            System.out.println("Requires rowNumberLimit parameter");
            return;
        }

        Integer rowNumberLimit = Integer.parseInt(args[0]);
        System.out.println("Starting with rowNumberLimit " + rowNumberLimit);
        new EvictionClient().run(rowNumberLimit);
    }

    private void run(long rowNumberLimit) {
        try (Ignite ignite = Ignition.start("ignite-client.xml")) {
            IgniteCache<Integer, Declaration> declsCache = ignite.cache("DeclarationCache");
            IgniteCache<Integer, DeclarationMaster> declsMasterCache = ignite.cache("DeclarationMasterCache");

            try (IgniteDataStreamer<Integer, Declaration> ds = ignite.dataStreamer(declsCache.getName())) {
                ds.allowOverwrite(true);

                while (true) {
                    try {
                        int size = declsCache.size(CachePeekMode.ALL);
                        if (size > rowNumberLimit) {
                            System.out.println("Size " + size + " exceeded " + rowNumberLimit + " which is the limit of allowed rows");

                            int declarationIdToEvict = getDeclarationIdToEvict(declsMasterCache);
                            declsMasterCache.remove(declarationIdToEvict);

                            System.out.println("Deleting for declaration id " + declarationIdToEvict);

                            try (FieldsQueryCursor<List<?>> cursor = declsCache.query(new SqlFieldsQuery("SELECT ROWID FROM \"DeclarationCache\".Declaration WHERE DECLARATIONID = ?").setArgs(declarationIdToEvict))) {
                                for (List<?> objects : cursor) {
                                    Integer rowId = (Integer)objects.get(0);
                                    ds.removeData(rowId);
                                }
                            }

                            System.out.println("Completed delete");
                        }
                        Thread.sleep(1000);
                    }
                    catch (IgniteFutureTimeoutException e) {
                        System.out.println("Future not ready");
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    /**
     * Returns the declaration ID that is the next for deletion.  This should be
     * @param declsMasterCache
     * @return
     */
    private int getDeclarationIdToEvict(IgniteCache<Integer, DeclarationMaster> declsMasterCache) {
        List<List<?>> list = declsMasterCache.query(new SqlFieldsQuery("SELECT DECLARATIONID FROM \"DeclarationMasterCache\".DECLARATIONMASTER LIMIT 1")).getAll();

        return (int)(Integer)list.get(0).get(0);
    }
}
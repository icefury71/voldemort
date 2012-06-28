package voldemort.client;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Random;
import java.util.concurrent.Callable;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import voldemort.versioning.Versioned;

/*
 * The AsyncMetadataVersionManager is used to track the Metadata version on the
 * cluster and if necessary Re-bootstrap the client.
 * 
 * During initialization, it will retrieve the current version of the store (or
 * the entire stores.xml depending upon granularity) and then periodically check
 * whether this has been updated. During init if the initial version turns out
 * to be null, it means that no change has been done to that store since it was
 * created. In this case, we assume version '0'.
 */

public class AsyncMetadataVersionManager implements Runnable {

    private static final String STORES_VERSION_KEY = "stores.xml";
    private static final String CLUSTER_VERSION_KEY = "cluster.xml";

    private final Logger logger = Logger.getLogger(this.getClass());
    private Versioned<Long> currentStoreVersion;
    private Versioned<Long> currentClusterVersion;
    private volatile boolean isRunning;
    private final Callable<Void> storeClientThunk;
    private long asyncMetadataCheckInterval;
    private final SystemStoreRepository sysRepository;

    // Random delta generator
    private final int DELTA_MAX = 2000;
    private final Random randomGenerator = new Random(System.currentTimeMillis());

    public AsyncMetadataVersionManager(SystemStoreRepository sysRepository,
                                       long asyncMetadataCheckInterval,
                                       Callable<Void> storeClientThunk) {
        this(null, sysRepository, asyncMetadataCheckInterval, storeClientThunk);
    }

    public AsyncMetadataVersionManager(Versioned<Long> initialStoreVersion,
                                       SystemStoreRepository sysRepository,
                                       long asyncMetadataCheckInterval,
                                       Callable<Void> storeClientThunk) {
        this.sysRepository = sysRepository;

        try {
            if(initialStoreVersion == null) {
                this.currentStoreVersion = this.sysRepository.getVersionStore()
                                                             .getSysStore(STORES_VERSION_KEY);
            } else {
                currentStoreVersion = initialStoreVersion;
            }
        } catch(Exception e) {
            logger.error("Exception while getting currentStoreVersion : " + e);
        }

        try {
            this.currentClusterVersion = this.sysRepository.getVersionStore()
                                                           .getSysStore(CLUSTER_VERSION_KEY);
        } catch(Exception e) {
            logger.error("Exception while getting currentClusterVersion : " + e);
        }

        // If the received version is null, assume version 0
        if(currentStoreVersion == null)
            currentStoreVersion = new Versioned<Long>((long) 0);
        if(currentClusterVersion == null)
            currentClusterVersion = new Versioned<Long>((long) 0);

        // Initialize and start the background check thread
        isRunning = true;

        Thread checkVersionThread = new Thread(this, "AsyncVersionCheckThread");
        checkVersionThread.setDaemon(true);
        checkVersionThread.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {

            public void uncaughtException(Thread t, Throwable e) {
                if(logger.isEnabledFor(Level.ERROR))
                    logger.error("Uncaught exception in Metadata Version check thread:", e);
            }
        });

        this.storeClientThunk = storeClientThunk;
        this.asyncMetadataCheckInterval = asyncMetadataCheckInterval;
        checkVersionThread.start();

    }

    public void destroy() {
        isRunning = false;
    }

    /*
     * This method checks for any update in the version for 'versionKey'. If
     * there is any change, it returns the new version. Otherwise it will return
     * a null.
     */
    public Versioned<Long> fetchNewVersion(String versionKey, Versioned<Long> curVersion) {
        try {
            Versioned<Long> newVersion = this.sysRepository.getVersionStore()
                                                           .getSysStore(versionKey);

            // If version obtained is null, the store is untouched. Continue
            if(newVersion != null) {
                logger.debug("MetadataVersion check => Obtained " + versionKey + " version : "
                             + newVersion);

                if(!newVersion.equals(curVersion)) {
                    return newVersion;
                }
            } else {
                logger.debug("Metadata unchanged after creation ...");
            }
        }

        // Swallow all exceptions here (we dont want to fail the client).
        catch(Exception e) {
            logger.info("Could not retrieve Metadata Version. Exception : " + e);
        }

        return null;
    }

    public void run() {
        Versioned<Long> newStoresVersion, newClusterVersion;
        while(!Thread.currentThread().isInterrupted() && isRunning) {
            newStoresVersion = newClusterVersion = null;

            try {
                Thread.sleep(asyncMetadataCheckInterval);
            } catch(InterruptedException e) {
                break;
            }

            newStoresVersion = fetchNewVersion(STORES_VERSION_KEY, currentStoreVersion);
            newClusterVersion = fetchNewVersion(CLUSTER_VERSION_KEY, currentClusterVersion);

            // If nothing has been updated, continue
            if(newStoresVersion == null && newClusterVersion == null) {
                continue;
            }

            logger.info("Metadata version mismatch detected.");

            // Determine a random delta delay between 0 to DELTA_MAX to sleep
            int delta = randomGenerator.nextInt(DELTA_MAX);

            try {
                logger.info("Sleeping for delta : " + delta + " (ms) before re-bootstrapping.");
                Thread.sleep(delta);
            } catch(InterruptedException e) {
                break;
            }

            try {
                this.storeClientThunk.call();

                if(newStoresVersion != null) {
                    currentStoreVersion = newStoresVersion;
                }

                if(newClusterVersion != null) {
                    currentClusterVersion = newClusterVersion;
                }
            } catch(Exception e) {
                e.printStackTrace();
                logger.info(e.getMessage());
            }

        }
    }

    public Versioned<Long> getStoreMetadataVersion() {
        return this.currentStoreVersion;
    }

    public Versioned<Long> getClusterMetadataVersion() {
        return this.currentClusterVersion;
    }

    public void updateMetadataVersions() {
        this.currentStoreVersion = fetchNewVersion(STORES_VERSION_KEY, null);
        this.currentClusterVersion = fetchNewVersion(CLUSTER_VERSION_KEY, null);
    }
}

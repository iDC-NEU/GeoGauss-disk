import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Transactional YCSB client.
 *
 * Mirrors the workload definition used by the Calvin adapter (applications/ycsb.h):
 * every transaction carries exactly 10 operations; MC issues 2 writes and 8 reads,
 * HC 5 writes and 5 reads, RO reads only. Keys are drawn from a Zipfian CDF
 * (theta 0.8 / 0.9, uniform for RO) over 1,000,000 rows and scattered through a
 * permutation, so hot rows are not the lowest ids.
 *
 * The YCSB table has ten columns: the key plus nine 100-byte value fields.
 *
 * Usage: YcsbTxnClient mode=load|run url=... user=... password=... [key=value ...]
 */
public final class YcsbTxnClient {
    private static final int FIELD_COUNT = 9;
    private static final int OPS_PER_TXN = 10;
    private static final int VALUE_LENGTH = 100;
    private static final long KEY_RANGE = 1000000L;

    private static String mode;
    private static String url;
    private static String user = "omm";
    private static String password = "";
    private static String table = "usertable";
    private static int threads = 256;
    private static int durationSeconds = 180;
    private static int warmupSeconds = 30;
    private static String mix = "MC";
    private static int writesPerTxn = 2;

    private static double[] cdf;

    private YcsbTxnClient() {
    }

    public static void main(String[] args) throws Exception {
        parseArgs(args);
        if (mode == null) {
            usage("mode is required");
        }
        Class.forName(System.getProperty("ycsb.driver", "org.postgresql.Driver"));
        buildCdf();
        if ("load".equals(mode)) {
            load();
        } else if ("run".equals(mode)) {
            run();
        } else {
            usage("unknown mode: " + mode);
        }
    }

    private static void usage(String message) {
        System.err.println("ERROR: " + message);
        System.err.println("usage: YcsbTxnClient mode=load|run url=JDBC_URL user=NAME password=PASS"
            + " [table=usertable] [threads=256] [duration=180] [warmup=30] [mix=MC|HC|RO]");
        System.exit(2);
    }

    private static void parseArgs(String[] args) {
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq <= 0) {
                usage("expected key=value, got: " + arg);
            }
            String key = arg.substring(0, eq);
            String value = arg.substring(eq + 1);
            if ("mode".equals(key)) {
                mode = value;
            } else if ("url".equals(key)) {
                url = value;
            } else if ("user".equals(key)) {
                user = value;
            } else if ("password".equals(key)) {
                password = value;
            } else if ("table".equals(key)) {
                table = value;
            } else if ("threads".equals(key)) {
                threads = Integer.parseInt(value);
            } else if ("duration".equals(key)) {
                durationSeconds = Integer.parseInt(value);
            } else if ("warmup".equals(key)) {
                warmupSeconds = Integer.parseInt(value);
            } else if ("mix".equals(key)) {
                mix = value.toUpperCase();
            } else {
                usage("unknown option: " + key);
            }
        }
        if (url == null) {
            usage("url is required");
        }
        if ("MC".equals(mix)) {
            writesPerTxn = 2;
        } else if ("HC".equals(mix)) {
            writesPerTxn = 5;
        } else if ("RO".equals(mix)) {
            writesPerTxn = 0;
        } else {
            usage("mix must be MC, HC or RO");
        }
    }

    /* Zipfian CDF, same shape as the Calvin client: P(rank) ~ rank^-theta. */
    private static void buildCdf() {
        final double theta = "MC".equals(mix) ? 0.8 : "HC".equals(mix) ? 0.9 : 0.0;
        if (theta == 0.0) {
            cdf = null;
            return;
        }
        cdf = new double[(int) KEY_RANGE];
        double sum = 0;
        for (int i = 1; i <= KEY_RANGE; i++) {
            sum += Math.pow(i, -theta);
            cdf[i - 1] = sum;
        }
        for (int i = 0; i < cdf.length; i++) {
            cdf[i] /= sum;
        }
    }

    private static Connection connect() throws SQLException {
        Connection conn = DriverManager.getConnection(url, user, password);
        conn.setAutoCommit(false);
        return conn;
    }

    private static void load() throws Exception {
        Connection conn = connect();
        Statement stmt = conn.createStatement();
        StringBuilder ddl = new StringBuilder();
        ddl.append("CREATE TABLE IF NOT EXISTS ").append(table).append(" (ycsb_key BIGINT PRIMARY KEY");
        for (int i = 0; i < FIELD_COUNT; i++) {
            ddl.append(", field").append(i).append(" VARCHAR(").append(VALUE_LENGTH).append(')');
        }
        ddl.append(')');
        stmt.execute(ddl.toString());
        conn.commit();
        stmt.close();

        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(table).append(" (ycsb_key");
        for (int i = 0; i < FIELD_COUNT; i++) {
            sql.append(", field").append(i);
        }
        sql.append(") VALUES (?");
        for (int i = 0; i < FIELD_COUNT; i++) {
            sql.append(", ?");
        }
        sql.append(')');

        PreparedStatement ps = conn.prepareStatement(sql.toString());
        Random random = new Random(7);
        long start = System.currentTimeMillis();
        for (long key = 1; key <= KEY_RANGE; key++) {
            ps.setLong(1, key);
            for (int i = 0; i < FIELD_COUNT; i++) {
                ps.setString(2 + i, randomValue(random));
            }
            ps.addBatch();
            if (key % 1000 == 0) {
                ps.executeBatch();
                conn.commit();
            }
            if (key % 100000 == 0) {
                System.out.println("loaded " + key + " rows");
            }
        }
        ps.executeBatch();
        conn.commit();
        ps.close();
        conn.close();
        System.out.println("load done: rows=" + KEY_RANGE + " elapsed_ms="
            + (System.currentTimeMillis() - start));
    }

    private static String randomValue(Random random) {
        char[] value = new char[VALUE_LENGTH];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) ('a' + random.nextInt(26));
        }
        return new String(value);
    }

    private static void run() throws Exception {
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicBoolean warmupCaptured = new AtomicBoolean(false);
        final AtomicLong commits = new AtomicLong();
        final AtomicLong warmupCommits = new AtomicLong();
        final AtomicLong errors = new AtomicLong();

        List<Worker> workers = new ArrayList<Worker>();
        for (int i = 0; i < threads; i++) {
            Worker worker = new Worker(i, running, commits, warmupCommits, errors);
            workers.add(worker);
            worker.start();
        }

        long start = System.currentTimeMillis();
        long warmupMs = warmupSeconds * 1000L;
        while (true) {
            Thread.sleep(100);
            long elapsed = System.currentTimeMillis() - start;
            if (warmupMs > 0 && elapsed >= warmupMs && warmupCaptured.compareAndSet(false, true)) {
                warmupCommits.set(commits.get());
                System.out.println("warmup done: " + warmupSeconds + "s, txns="
                    + warmupCommits.get());
            }
            if (elapsed - warmupMs >= durationSeconds * 1000L) {
                break;
            }
        }
        running.set(false);
        for (Worker worker : workers) {
            worker.join();
        }
        long measuredMs = Math.max(1L, System.currentTimeMillis() - start - warmupMs);
        long txns = commits.get() - warmupCommits.get();

        long[] merged = new long[Worker.LATENCY_BUCKETS];
        for (Worker worker : workers) {
            worker.mergeLatency(merged);
        }
        long samples = 0;
        for (long v : merged) {
            samples += v;
        }
        System.out.println("mix=" + mix + " threads=" + threads + " ops_per_txn=" + OPS_PER_TXN
            + " measured_s=" + String.format("%.1f", measuredMs / 1000.0));
        System.out.println("txns=" + txns + " errors=" + errors.get()
            + " tps=" + String.format("%.1f", txns * 1000.0 / measuredMs)
            + " ops_per_s=" + String.format("%.1f", txns * (double) OPS_PER_TXN * 1000.0 / measuredMs));
        System.out.println("latency_ms: p50=" + percentile(merged, samples, 0.50)
            + " p95=" + percentile(merged, samples, 0.95)
            + " p99=" + percentile(merged, samples, 0.99));
    }

    private static long percentile(long[] hist, long total, double p) {
        if (total <= 0) {
            return 0;
        }
        long target = (long) Math.ceil(total * p);
        long seen = 0;
        for (int i = 0; i < hist.length; i++) {
            seen += hist[i];
            if (seen >= target) {
                return i;
            }
        }
        return hist.length - 1;
    }

    private static final class Worker extends Thread {
        static final int LATENCY_BUCKETS = 10000;

        private final AtomicBoolean running;
        private final AtomicLong commits;
        private final AtomicLong warmupCommits;
        private final AtomicLong errors;
        private final long[] latency = new long[LATENCY_BUCKETS];
        private final Random random;
        private final String[] values = new String[1024];
        private final boolean[] writeFlags = new boolean[OPS_PER_TXN];
        private final PreparedStatement[] reads = new PreparedStatement[FIELD_COUNT];
        private final PreparedStatement[] updates = new PreparedStatement[FIELD_COUNT];

        Worker(int id, AtomicBoolean running, AtomicLong commits, AtomicLong warmupCommits,
            AtomicLong errors) {
            this.running = running;
            this.commits = commits;
            this.warmupCommits = warmupCommits;
            this.errors = errors;
            this.random = new Random(115827L + 1009L * id);
            Random valueRandom = new Random(52421L);
            for (int i = 0; i < values.length; i++) {
                values[i] = randomValue(valueRandom);
            }
            setDaemon(true);
        }

        @Override
        public void run() {
            Connection conn = null;
            try {
                conn = connect();
                for (int f = 0; f < FIELD_COUNT; f++) {
                    reads[f] = conn.prepareStatement(
                        "SELECT field" + f + " FROM " + table + " WHERE ycsb_key = ?");
                    updates[f] = conn.prepareStatement(
                        "UPDATE " + table + " SET field" + f + " = ? WHERE ycsb_key = ?");
                }
                while (running.get()) {
                    long begin = System.nanoTime();
                    try {
                        executeTransaction(conn);
                        conn.commit();
                        commits.incrementAndGet();
                        long usec = (System.nanoTime() - begin) / 1000L;
                        int bucket = (int) Math.min(LATENCY_BUCKETS - 1, usec / 1000L);
                        latency[bucket]++;
                    } catch (SQLException e) {
                        errors.incrementAndGet();
                        try {
                            conn.rollback();
                        } catch (SQLException ignored) {
                            // connection is unusable; the next transaction will fail too
                        }
                    }
                }
            } catch (SQLException e) {
                errors.incrementAndGet();
            } finally {
                for (int f = 0; f < FIELD_COUNT; f++) {
                    closeQuietly(reads[f]);
                    closeQuietly(updates[f]);
                }
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException ignored) {
                        // nothing to do
                    }
                }
            }
        }

        private static void closeQuietly(PreparedStatement ps) {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException ignored) {
                    // nothing to do
                }
            }
        }

        private void executeTransaction(Connection conn) throws SQLException {
            for (int i = 0; i < OPS_PER_TXN; i++) {
                writeFlags[i] = i < writesPerTxn;
            }
            for (int i = OPS_PER_TXN - 1; i > 0; i--) {
                int j = random.nextInt(i + 1);
                boolean tmp = writeFlags[i];
                writeFlags[i] = writeFlags[j];
                writeFlags[j] = tmp;
            }
            for (int i = 0; i < OPS_PER_TXN; i++) {
                long key = nextKey();
                int field = random.nextInt(FIELD_COUNT);
                if (writeFlags[i]) {
                    update(conn, key, field);
                } else {
                    read(conn, key, field);
                }
            }
        }

        private long nextKey() {
            if (cdf == null) {
                return 1 + (long) (random.nextDouble() * KEY_RANGE);
            }
            double draw = random.nextDouble();
            int low = 0;
            int high = cdf.length - 1;
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (cdf[mid] < draw) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            /* 与 Calvin 客户端相同的置换，热点不落在最小的 id 上 */
            return ((long) low * 735137L + 89317L) % KEY_RANGE + 1;
        }

        private void read(Connection conn, long key, int field) throws SQLException {
            PreparedStatement ps = reads[field];
            ps.setLong(1, key);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                rs.getString(1);
            }
            rs.close();
        }

        private void update(Connection conn, long key, int field) throws SQLException {
            PreparedStatement ps = updates[field];
            ps.setString(1, values[random.nextInt(values.length)]);
            ps.setLong(2, key);
            ps.executeUpdate();
        }

        void mergeLatency(long[] into) {
            for (int i = 0; i < LATENCY_BUCKETS; i++) {
                into[i] += latency[i];
            }
        }
    }
}

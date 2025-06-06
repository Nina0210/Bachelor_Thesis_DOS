// JVM Arguments: -Xmx5g
public class SparkMemoryCalculation {

    private static final long MB = 1024 * 1024;
    private static final long RESERVED_SYSTEM_MEMORY_BYTES = 300 * MB;
    private static final double SparkMemoryStorageFraction = 0.5;
    private static final double SparkMemoryFraction = 0.6;

    public static void main(String[] args) {

        long systemMemory = Runtime.getRuntime().maxMemory();
        long usableMemory = systemMemory - RESERVED_SYSTEM_MEMORY_BYTES;
        long sparkMemory = convertDoubletLong(usableMemory * SparkMemoryFraction);
        long userMemory = convertDoubletLong(usableMemory * (1 - SparkMemoryFraction));

        long storageMemory = convertDoubletLong(sparkMemory * SparkMemoryStorageFraction);
        long executionMemory = convertDoubletLong(sparkMemory * (1 - SparkMemoryStorageFraction));

        printMemoryInMB("Heap Memory\t", systemMemory);
        printMemoryInMB("Reserved Memory", RESERVED_SYSTEM_MEMORY_BYTES);
        printMemoryInMB("Usable Memory\t", usableMemory);
        printMemoryInMB("User Memory\t", userMemory);
        printMemoryInMB("Spark Memory\t", sparkMemory);

        printMemoryInMB("Storage Memory\t", storageMemory);
        printMemoryInMB("Execution Memory", executionMemory);

        System.out.println();
        printStorageMemoryInMB("Spark Storage Memory", sparkMemory);
        printStorageMemoryInMB("Storage Memory UI", storageMemory);
        printStorageMemoryInMB("Execution Memory UI", executionMemory);
    }

    private static void printMemoryInMB(String type, long memory) {
        System.out.println(type + " \t=\t"+ (memory/MB) +" MB");
    }

    private static void printStorageMemoryInMB(String type, long memory) {
        System.out.println(type + " \t=\t"+ (memory/(1000*1000)) +" MB");
    }

    private static Long convertDoubletLong(double val) {
        return Double.valueOf(val).longValue();
    }
}
package pl.twojserwer.dynamosync;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Warstwa dostepu do DynamoDB. Wszystkie wywolania sieciowe leca na osobnej puli
 * watkow (executor), nigdy na glownym watku serwera - inaczej kazdy lag AWS/OVH
 * bedzie zamrazal caly serwer.
 *
 * Optymalizacje kosztow:
 *  - Zapisujemy do DynamoDB TYLKO gdy PlayerData jest "dirty" (cos sie realnie
 *    zmienilo od ostatniego zapisu). W DynamoDB placi sie za WCU/RCU, nie za
 *    liczbe wywolan API, wiec pomijanie "czystych" rekordow przy autosave to
 *    najprostszy sposob na realne ograniczenie kosztu.
 *  - Autosave uzywa BatchWriteItem (do 25 itemow na request) zamiast N
 *    pojedynczych PutItem - mniej requestow HTTP, mniejszy narzut i mniejsze
 *    ryzyko throttlingu przy duzej liczbie graczy online.
 *  - BatchWriteItem zwraca "unprocessed items" przy throttlingu zamiast
 *    rzucac wyjatek - dorzucamy retry z prostym backoffem zamiast tracic zapis.
 */
public class PlayerDataManager {

    private static final int BATCH_WRITE_LIMIT = 25; // limit DynamoDB dla BatchWriteItem
    private static final int MAX_BATCH_RETRIES = 5;

    private final DynamoDbClient client;
    private final String tableName;
    private final ExecutorService executor;
    private final Logger logger;
    private final boolean debug;

    private final Map<UUID, PlayerData> cache = new ConcurrentHashMap<>();

    // --- statystyki / debug ---
    private final AtomicLong totalLoadsFromDb = new AtomicLong();
    private final AtomicLong totalLoadsFromCache = new AtomicLong();
    private final AtomicLong totalWritesSent = new AtomicLong();       // realne PutItem/BatchWriteItem-item wyslane do AWS
    private final AtomicLong totalWritesSkippedClean = new AtomicLong(); // zapisy pominiete bo dane byly "czyste" (oszczednosc WCU)
    private final AtomicLong totalErrors = new AtomicLong();
    private final AtomicLong totalAutosaveRuns = new AtomicLong();
    private volatile long lastAutosaveDurationMs = -1;
    private volatile long lastAutosaveDirtyCount = -1;

    public PlayerDataManager(DynamoDbClient client, String tableName, ExecutorService executor,
                              Logger logger, boolean debug) {
        this.client = client;
        this.tableName = tableName;
        this.executor = executor;
        this.logger = logger;
        this.debug = debug;
    }

    /** Tworzy tabele (partition key: uuid, String), jesli jeszcze nie istnieje. Blokujace - wolaj tylko przy starcie. */
    public void createTableIfMissing() {
        try {
            client.describeTable(DescribeTableRequest.builder().tableName(tableName).build());
            logger.info("[DynamoSync] Tabela '" + tableName + "' juz istnieje.");
        } catch (ResourceNotFoundException e) {
            logger.info("[DynamoSync] Tabela '" + tableName + "' nie istnieje - tworze (PAY_PER_REQUEST)...");
            client.createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(KeySchemaElement.builder().attributeName("uuid").keyType(KeyType.HASH).build())
                    .attributeDefinitions(AttributeDefinition.builder()
                            .attributeName("uuid").attributeType(ScalarAttributeType.S).build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build());
            client.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build());
            logger.info("[DynamoSync] Tabela utworzona.");
        }
    }

    /** Laduje dane gracza z DynamoDB (albo z cache, jesli juz sa) i wrzuca do cache. */
    public CompletableFuture<PlayerData> load(UUID uuid, String usernameHint) {
        PlayerData cached = cache.get(uuid);
        if (cached != null) {
            totalLoadsFromCache.incrementAndGet();
            if (debug) {
                logger.info("[DynamoSync] [debug] load(" + usernameHint + "): trafienie w cache, RCU=0 (cache size="
                        + cache.size() + ")");
            }
            return CompletableFuture.completedFuture(cached);
        }
        long startNanos = System.nanoTime();
        return CompletableFuture.supplyAsync(() -> {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("uuid", AttributeValue.builder().s(uuid.toString()).build());
            GetItemResponse resp = client.getItem(GetItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .build());
            PlayerData playerData = PlayerData.fromItem(uuid, usernameHint, resp.item());
            cache.put(uuid, playerData);
            totalLoadsFromDb.incrementAndGet();
            if (debug) {
                long ms = (System.nanoTime() - startNanos) / 1_000_000;
                logger.info("[DynamoSync] [debug] Zaladowano z DynamoDB gracza " + usernameHint + " (" + uuid
                        + ") w " + ms + " ms, znaleziono item=" + resp.hasItem() + ", cache size=" + cache.size());
            }
            return playerData;
        }, executor).exceptionally(ex -> {
            totalErrors.incrementAndGet();
            logger.log(Level.WARNING, "[DynamoSync] Blad ladowania danych gracza " + uuid, ex);
            PlayerData fallback = new PlayerData(uuid, usernameHint);
            cache.put(uuid, fallback);
            return fallback;
        });
    }

    /**
     * Zapisuje gracza do DynamoDB asynchronicznie, ale TYLKO jesli dane sa "dirty".
     * To jest metoda, ktorej powinny uzywac autosave i quit - unika placenia za
     * WCU, gdy nic sie nie zmienilo od ostatniego zapisu.
     */
    public CompletableFuture<Void> saveIfDirty(PlayerData playerData) {
        if (!playerData.isDirty()) {
            totalWritesSkippedClean.incrementAndGet();
            if (debug) {
                logger.info("[DynamoSync] [debug] Pomijam zapis " + playerData.getUsername()
                        + " - brak zmian od ostatniego zapisu (oszczedzone WCU).");
            }
            return CompletableFuture.completedFuture(null);
        }
        return save(playerData);
    }

    /** Wymuszony zapis (np. po recznej zmianie przez komende /playerdata set). Zawsze leci do DynamoDB. */
    public CompletableFuture<Void> save(PlayerData playerData) {
        long startNanos = System.nanoTime();
        return CompletableFuture.runAsync(() -> {
            try {
                client.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(playerData.toItem())
                        .build());
                playerData.clearDirty();
                totalWritesSent.incrementAndGet();
                if (debug) {
                    long ms = (System.nanoTime() - startNanos) / 1_000_000;
                    logger.info("[DynamoSync] [debug] Zapisano gracza " + playerData.getUsername()
                            + " w " + ms + " ms (WCU zuzyte, ~" + estimateItemSizeBytes(playerData) + " B)");
                }
            } catch (Exception ex) {
                totalErrors.incrementAndGet();
                logger.log(Level.WARNING, "[DynamoSync] Blad zapisu danych gracza " + playerData.getUsername(), ex);
            }
        }, executor);
    }

    /**
     * Zapisuje w tle tylko "brudnych" graczy z cache, uzywajac BatchWriteItem
     * (paczki po max 25 itemow) zamiast N osobnych PutItem. Wywolywane przez
     * autosave timer. Zwraca liczbe faktycznie zapisanych (dirty) graczy.
     */
    public CompletableFuture<Integer> autosaveDirty() {
        return CompletableFuture.supplyAsync(() -> {
            long startNanos = System.nanoTime();
            List<PlayerData> dirty = new ArrayList<>();
            for (PlayerData pd : cache.values()) {
                if (pd.isDirty()) {
                    dirty.add(pd);
                }
            }
            totalAutosaveRuns.incrementAndGet();

            if (dirty.isEmpty()) {
                lastAutosaveDurationMs = (System.nanoTime() - startNanos) / 1_000_000;
                lastAutosaveDirtyCount = 0;
                if (debug) {
                    logger.info("[DynamoSync] [debug] Autosave: 0/" + cache.size()
                            + " graczy do zapisania (wszyscy czysci, 0 WCU zuzyte).");
                }
                return 0;
            }

            int saved = batchWriteAll(dirty);

            long ms = (System.nanoTime() - startNanos) / 1_000_000;
            lastAutosaveDurationMs = ms;
            lastAutosaveDirtyCount = saved;
            if (debug) {
                logger.info("[DynamoSync] [debug] Autosave: zapisano " + saved + "/" + dirty.size()
                        + " brudnych graczy (z " + cache.size() + " w cache) w " + ms + " ms.");
            }
            return saved;
        }, executor).exceptionally(ex -> {
            totalErrors.incrementAndGet();
            logger.log(Level.WARNING, "[DynamoSync] Blad autosave", ex);
            return 0;
        });
    }

    /** Dzieli liste na paczki po BATCH_WRITE_LIMIT i wysyla BatchWriteItem z retry na unprocessed items. */
    private int batchWriteAll(List<PlayerData> players) {
        int savedCount = 0;
        for (int i = 0; i < players.size(); i += BATCH_WRITE_LIMIT) {
            List<PlayerData> chunk = players.subList(i, Math.min(i + BATCH_WRITE_LIMIT, players.size()));
            List<WriteRequest> requests = new ArrayList<>(chunk.size());
            for (PlayerData pd : chunk) {
                requests.add(WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(pd.toItem()).build())
                        .build());
            }

            Map<String, List<WriteRequest>> toSend = new HashMap<>();
            toSend.put(tableName, requests);

            int attempt = 0;
            while (!toSend.isEmpty() && attempt < MAX_BATCH_RETRIES) {
                try {
                    BatchWriteItemResponse resp = client.batchWriteItem(BatchWriteItemRequest.builder()
                            .requestItems(toSend)
                            .build());
                    toSend = resp.unprocessedItems();
                    if (toSend != null && !toSend.isEmpty()) {
                        attempt++;
                        long backoffMs = 100L * (1L << attempt); // proste exponential backoff
                        if (debug) {
                            logger.info("[DynamoSync] [debug] BatchWriteItem: throttling, "
                                    + toSend.getOrDefault(tableName, List.of()).size()
                                    + " itemow do ponowienia (proba " + attempt + "), czekam " + backoffMs + " ms.");
                        }
                        Thread.sleep(backoffMs);
                    } else {
                        toSend = Map.of();
                    }
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ex) {
                    totalErrors.incrementAndGet();
                    logger.log(Level.WARNING, "[DynamoSync] Blad BatchWriteItem (proba " + attempt + ")", ex);
                    break;
                }
            }

            if (toSend != null && !toSend.isEmpty()) {
                logger.warning("[DynamoSync] BatchWriteItem: " + toSend.getOrDefault(tableName, List.of()).size()
                        + " itemow nie udalo sie zapisac po " + MAX_BATCH_RETRIES + " probach (throttling/blad).");
            }

            for (PlayerData pd : chunk) {
                pd.clearDirty();
                savedCount++;
                totalWritesSent.incrementAndGet();
            }
        }
        return savedCount;
    }

    /** Zwraca dane gracza z cache (bez zapytania do sieci) - null jesli jeszcze nie zaladowane. */
    public PlayerData getCached(UUID uuid) {
        return cache.get(uuid);
    }

    public void evictFromCache(UUID uuid) {
        cache.remove(uuid);
        if (debug) {
            logger.info("[DynamoSync] [debug] Usunieto z cache " + uuid + " (cache size=" + cache.size() + ")");
        }
    }

    /**
     * Blokujacy zapis wszystkich BRUDNYCH graczy w cache - uzywane wylacznie w onDisable.
     * Uzywa BatchWriteItem tak samo jak autosave, zeby zamkniecie serwera bylo szybkie
     * nawet z wieloma graczami online.
     */
    public void saveAllBlocking() {
        List<PlayerData> dirty = new ArrayList<>();
        for (PlayerData pd : cache.values()) {
            if (pd.isDirty()) {
                dirty.add(pd);
            }
        }
        if (dirty.isEmpty()) {
            logger.info("[DynamoSync] Shutdown: brak zmian do zapisania (0 WCU zuzyte).");
            return;
        }
        int saved = batchWriteAll(dirty);
        logger.info("[DynamoSync] Shutdown: zapisano " + saved + "/" + dirty.size() + " brudnych graczy ("
                + (cache.size() - dirty.size()) + " pominietych - bez zmian).");
    }

    public Map<UUID, PlayerData> getCacheSnapshot() {
        return cache;
    }

    public int getCacheSize() {
        return cache.size();
    }

    /** Bardzo przyblizony rozmiar itemu w bajtach - tylko do celow debug/logow, nie do rozliczen. */
    private int estimateItemSizeBytes(PlayerData pd) {
        int size = 64; // stale liczbowe pola + narzut
        size += pd.getUsername() != null ? pd.getUsername().length() : 0;
        for (Map.Entry<String, String> e : pd.getCustomData().entrySet()) {
            size += e.getKey().length() + (e.getValue() != null ? e.getValue().length() : 0);
        }
        return size;
    }

    /** Migawka statystyk do celow diagnostycznych (komenda /playerdata debug, logi). */
    public DebugStats getDebugStats() {
        return new DebugStats(
                cache.size(),
                totalLoadsFromDb.get(),
                totalLoadsFromCache.get(),
                totalWritesSent.get(),
                totalWritesSkippedClean.get(),
                totalErrors.get(),
                totalAutosaveRuns.get(),
                lastAutosaveDurationMs,
                lastAutosaveDirtyCount
        );
    }

    public record DebugStats(
            int cacheSize,
            long totalLoadsFromDb,
            long totalLoadsFromCache,
            long totalWritesSent,
            long totalWritesSkippedClean,
            long totalErrors,
            long totalAutosaveRuns,
            long lastAutosaveDurationMs,
            long lastAutosaveDirtyCount
    ) {}
}

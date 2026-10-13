package pl.twojserwer.dynamosync;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Warstwa dostepu do DynamoDB (tabela "Profiles"). Wszystkie wywolania sieciowe
 * leca na osobnej puli watkow (executor), nigdy na glownym watku serwera -
 * inaczej kazdy lag AWS/OVH bedzie zamrazal caly serwer.
 *
 * === Ochrona przed zlosliwymi/uszkadzajacymi zapytaniami ===
 * DynamoDB nie ma klasycznego "SQL injection" (API klucz-wartosc, nie SQL), ale:
 *  - wejscie jest walidowane w {@link InputValidator} i w {@link PlayerData#setCustom} (limity dlugosci,
 *    whitelist znakow, zarezerwowane nazwy, max liczba pol) ORAZ jeszcze raz twardym limitem rozmiaru
 *    calego itemu (w BAJTACH UTF-8) tuz przed wyslaniem do AWS,
 *  - limit zapytan per IP ({@link IpRateLimiter}),
 *  - odczyty z bazy sa silnie spojne (consistentRead) - nieaktualny odczyt po szybkim wyjsciu/wejsciu
 *    nie moze nadpisac swiezszych danych starymi.
 *
 * === Ochrona integralnosci danych (v1.1.0) ===
 *  - Nieudany odczyt NIE zastepuje juz prawdziwego rekordu pustym profilem: gracz dostaje tymczasowy profil
 *    awaryjny, ktory nigdy nie jest zapisywany, a w tle ponawiamy odczyt i dopiero wtedy laczymy zmiany
 *    z sesji z prawdziwymi danymi.
 *  - BatchWriteItem oznacza jako "zapisane" tylko to, co baza faktycznie potwierdzila; reszta zostaje
 *    dirty i idzie przy nastepnym autosave (wczesniej bledy/throttling po cichu gubily dane).
 *  - Wpis wychodzacego gracza zostaje w cache, dopoki nie jest zapisany - nieudany zapis przy wyjsciu
 *    nie kasuje juz danych.
 *  - Komendy admina (get/stats) czytaja baze bez zasmiecania cache i bez tworzenia pustych profili.
 *
 * Optymalizacje kosztow:
 *  - Zapisujemy do DynamoDB TYLKO gdy PlayerData jest "dirty".
 *  - Autosave uzywa BatchWriteItem (do 25 itemow na request).
 *  - BatchWriteItem retry z prostym backoffem na unprocessed items.
 */
public class PlayerDataManager {

    private static final int BATCH_WRITE_LIMIT = 25; // limit DynamoDB dla BatchWriteItem
    private static final int MAX_BATCH_RETRIES = 5;
    private static final int MAX_QUICK_RECOVERY_ATTEMPTS = 3; // dalej odzyskiwanie ponawia autosave
    private static final int MAX_ITEM_SIZE_BYTES = 380 * 1024; // margines ponizej twardego limitu DynamoDB (400 KB)

    private final DynamoDbClient client;
    private final String tableName;
    private final ExecutorService executor;
    private final Logger logger;
    private final boolean debug;

    private final Map<UUID, PlayerData> cache = new ConcurrentHashMap<>();

    // Limit zapytan per IP (null = wylaczony). Ustawiany z DynamoSyncPlugin po utworzeniu managera.
    private volatile IpRateLimiter rateLimiter;

    // --- statystyki / debug ---
    private final AtomicLong totalLoadsFromDb = new AtomicLong();
    private final AtomicLong totalLoadsFromCache = new AtomicLong();
    private final AtomicLong totalWritesSent = new AtomicLong();       // realne PutItem/BatchWriteItem-item wyslane do AWS
    private final AtomicLong totalWritesSkippedClean = new AtomicLong(); // zapisy pominiete bo dane byly "czyste" (oszczednosc WCU)
    private final AtomicLong totalWritesRejectedInvalid = new AtomicLong(); // zapisy odrzucone przez walidacje (zamiast lecec do AWS)
    private final AtomicLong totalErrors = new AtomicLong();
    private final AtomicLong totalAutosaveRuns = new AtomicLong();
    private final AtomicLong totalDegradedLoads = new AtomicLong();    // odczyty zakonczone trybem awaryjnym
    private final AtomicLong totalRecovered = new AtomicLong();        // profile awaryjne odzyskane z bazy
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

    public void setRateLimiter(IpRateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public IpRateLimiter getRateLimiter() {
        return rateLimiter;
    }

    /**
     * Tworzy tabele "Profiles" (partition key: uuid, String bez myslnikow), jesli jeszcze nie istnieje.
     * Nowa tabela dostaje ochrone przed usunieciem (deletion protection) - przypadkowe/zlosliwe
     * DeleteTable nie przejdzie, dopoki ktos swiadomo jej nie wylaczy. Blokujace - wolaj tylko przy starcie.
     */
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
                    .deletionProtectionEnabled(true)
                    .build());
            client.waiter().waitUntilTableExists(DescribeTableRequest.builder().tableName(tableName).build());
            logger.info("[DynamoSync] Tabela utworzona (z wlaczona ochrona przed usunieciem).");
        }
    }

    /**
     * Waliduje pojedyncze pole custom (klucz + wartosc) przed dopuszczeniem go do zapisu.
     * Zwraca null jesli OK, albo komunikat bledu do pokazania nadawcy komendy.
     */
    public String validateCustomField(String key, String value) {
        return InputValidator.validateCustomField(key, value);
    }

    /**
     * Laduje dane gracza bez limitu IP (zadania wewnetrzne, np. inny plugin przez API).
     * Dla wejscia gracza i komend uzywaj {@link #load(UUID, String, String)}.
     */
    public CompletableFuture<PlayerData> load(UUID uuid, String usernameHint) {
        return load(uuid, usernameHint, null);
    }

    /**
     * Laduje dane gracza z DynamoDB (albo z cache, jesli juz sa) i wrzuca do cache.
     *
     * @param requesterIp adres IP, ktoremu zaliczamy zapytanie do limitu (gracz wchodzacy na serwer
     *                    albo admin wpisujacy komende). null = bez limitu (konsola, zadania wewnetrzne).
     *                    Trafienie w cache NIE jest zapytaniem do bazy, wiec nie zuzywa limitu.
     * @return future zakonczony wyjatkiem {@link RateLimitExceededException}, jesli IP wyczerpalo limit.
     *         Przy bledzie bazy future konczy sie normalnie z profilem awaryjnym ({@link PlayerData#isDegraded()}).
     */
    public CompletableFuture<PlayerData> load(UUID uuid, String usernameHint, String requesterIp) {
        PlayerData cached = cache.get(uuid);
        if (cached != null) {
            totalLoadsFromCache.incrementAndGet();
            if (debug) {
                logger.info("[DynamoSync] [debug] load(" + usernameHint + "): trafienie w cache, RCU=0 (cache size="
                        + cache.size() + ")");
            }
            return CompletableFuture.completedFuture(cached);
        }

        RateLimitExceededException denied = checkLimit(requesterIp, usernameHint);
        if (denied != null) {
            return CompletableFuture.failedFuture(denied);
        }

        long startNanos = System.nanoTime();
        return CompletableFuture.supplyAsync(() -> {
            PlayerData fetched = fetchFromDb(uuid, usernameHint);
            totalLoadsFromDb.incrementAndGet();
            PlayerData result = fetched;
            PlayerData existing = cache.putIfAbsent(uuid, fetched);
            if (existing != null) {
                // Rownolegly load wygral wyscig. Jesli jego wynik jest awaryjny, a nasz prawdziwy - odzyskujemy.
                if (existing.isDegraded() && existing.recoverFrom(fetched)) {
                    totalRecovered.incrementAndGet();
                }
                result = existing;
            }
            if (debug) {
                long ms = (System.nanoTime() - startNanos) / 1_000_000;
                logger.info("[DynamoSync] [debug] Zaladowano z DynamoDB gracza " + usernameHint + " (" + uuid
                        + ") w " + ms + " ms, cache size=" + cache.size());
            }
            return result;
        }, executor).exceptionally(ex -> {
            totalErrors.incrementAndGet();
            totalDegradedLoads.incrementAndGet();
            logger.log(Level.WARNING, "[DynamoSync] Blad ladowania danych gracza " + uuid
                    + " - tryb awaryjny (dane NIE beda zapisywane, dopoki nie uda sie odczytac prawdziwego rekordu).", ex);
            PlayerData fallback = PlayerData.degradedFallback(uuid, usernameHint);
            PlayerData existing = cache.putIfAbsent(uuid, fallback);
            if (existing != null) {
                return existing;
            }
            scheduleRecovery(fallback, 1);
            return fallback;
        });
    }

    /**
     * Odczyt "tylko do podgladu" (komendy admina get/stats): korzysta z cache, jesli gracz tam jest,
     * w przeciwnym razie czyta baze, ale NIE wkłada wyniku do cache i NIE tworzy pustego profilu dla
     * nieistniejacego rekordu. Dzieki temu literowka w nicku nie zasmieca ani cache, ani tabeli.
     * Przy bledzie bazy future konczy sie wyjatkiem (admin dostaje komunikat zamiast fałszywych zer).
     */
    public CompletableFuture<PlayerData> loadReadOnly(UUID uuid, String usernameHint, String requesterIp) {
        PlayerData cached = cache.get(uuid);
        if (cached != null) {
            totalLoadsFromCache.incrementAndGet();
            return CompletableFuture.completedFuture(cached);
        }
        RateLimitExceededException denied = checkLimit(requesterIp, usernameHint);
        if (denied != null) {
            return CompletableFuture.failedFuture(denied);
        }
        return CompletableFuture.supplyAsync(() -> {
            PlayerData fetched = fetchFromDb(uuid, usernameHint);
            totalLoadsFromDb.incrementAndGet();
            return fetched;
        }, executor);
    }

    /** Sprawdza (i zuzywa) limit IP. null = wolno, w przeciwnym razie gotowy wyjatek do zwrocenia. */
    private RateLimitExceededException checkLimit(String requesterIp, String usernameHint) {
        IpRateLimiter limiter = rateLimiter;
        if (limiter != null && !limiter.tryAcquire(requesterIp)) {
            long retryMs = limiter.retryAfterMillis(requesterIp);
            if (debug) {
                logger.info("[DynamoSync] [debug] load(" + usernameHint + "): odrzucono - IP " + requesterIp
                        + " przekroczylo limit zapytan (jeszcze " + ((retryMs + 999) / 1000) + " s).");
            }
            return new RateLimitExceededException(retryMs);
        }
        return null;
    }

    /** Blokujacy GetItem (silnie spojny) + parsowanie. Wolac tylko z watku executora. Rzuca przy bledzie bazy. */
    private PlayerData fetchFromDb(UUID uuid, String usernameHint) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("uuid", AttributeValue.builder().s(PlayerData.uuidWithoutDashes(uuid)).build());
        GetItemResponse resp = client.getItem(GetItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .consistentRead(true)
                .build());
        return PlayerData.fromItem(uuid, usernameHint, resp.item());
    }

    // ------------------------------------------------------------------------------------------
    // Tryb awaryjny: odzyskiwanie
    // ------------------------------------------------------------------------------------------

    /** Blokujaco odczytuje prawdziwy rekord i laczy go z profilem awaryjnym. Rzuca przy bledzie bazy. */
    private void recover(PlayerData degraded) {
        PlayerData persisted = fetchFromDb(degraded.getUuid(), degraded.getUsername());
        if (degraded.recoverFrom(persisted)) {
            totalRecovered.incrementAndGet();
            logger.info("[DynamoSync] Odzyskano profil " + degraded.getUsername()
                    + " z bazy - zmiany z trybu awaryjnego polaczone, zapis nastapi przy najblizszym autosave/wyjsciu.");
        }
    }

    /** Szybkie ponowienia (5 s, 10 s, 20 s); jesli wszystkie padna, odzyskiwanie probuje dalej autosave. */
    private void scheduleRecovery(PlayerData fallback, int attempt) {
        long delaySeconds = 5L << (attempt - 1);
        CompletableFuture.runAsync(() -> {
            if (!fallback.isDegraded() || cache.get(fallback.getUuid()) != fallback) {
                return; // juz odzyskany albo gracz wyszedl i wpis usunieto
            }
            try {
                recover(fallback);
            } catch (Exception ex) {
                totalErrors.incrementAndGet();
                if (attempt < MAX_QUICK_RECOVERY_ATTEMPTS) {
                    scheduleRecovery(fallback, attempt + 1);
                } else {
                    logger.warning("[DynamoSync] Nie udalo sie odzyskac profilu " + fallback.getUsername()
                            + " po " + attempt + " probach (" + ex.getClass().getSimpleName()
                            + ") - ponowie przy autosave. Dane tego gracza NIE sa zapisywane, zeby nie nadpisac bazy.");
                }
            }
        }, CompletableFuture.delayedExecutor(delaySeconds, TimeUnit.SECONDS, executor));
    }

    private void recoverDegradedEntries() {
        for (PlayerData pd : cache.values()) {
            if (pd.isDegraded()) {
                try {
                    recover(pd);
                } catch (Exception ex) {
                    totalErrors.incrementAndGet();
                    logger.warning("[DynamoSync] Profil " + pd.getUsername() + " nadal w trybie awaryjnym: "
                            + ex.getClass().getSimpleName());
                }
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Zapis
    // ------------------------------------------------------------------------------------------

    /**
     * Zapisuje gracza do DynamoDB asynchronicznie, ale TYLKO jesli dane sa "dirty".
     * To jest metoda, ktorej powinny uzywac autosave i quit - unika placenia za
     * WCU, gdy nic sie nie zmienilo od ostatniego zapisu.
     */
    public CompletableFuture<Void> saveIfDirty(PlayerData playerData) {
        if (!playerData.isDirty() && !playerData.isDegraded()) {
            totalWritesSkippedClean.incrementAndGet();
            if (debug) {
                logger.info("[DynamoSync] [debug] Pomijam zapis " + playerData.getUsername()
                        + " - brak zmian od ostatniego zapisu (oszczedzone WCU).");
            }
            return CompletableFuture.completedFuture(null);
        }
        return save(playerData);
    }

    /**
     * Wymuszony zapis (np. po recznej zmianie przez komende /playerdata set). Zawsze leci do DynamoDB,
     * ale najpierw przechodzi przez guard rozmiaru itemu - zbyt duzy item jest odrzucany PRZED
     * wyslaniem do AWS (zamiast dowiadywac sie o tym z wyjatku po fakcie).
     */
    public CompletableFuture<Void> save(PlayerData playerData) {
        return save(playerData, playerData.getSessionIp());
    }

    /**
     * Jak {@link #save(PlayerData)}, ale zapis zalicza sie do limitu podanego IP (np. admina, ktory
     * wpisal /playerdata set). Zapis NIGDY nie jest odrzucany przez limit - odrzucony zapis to utrata
     * danych - zuzywa on tylko budzet IP (zob. {@link IpRateLimiter#record(String)}).
     * Profil w trybie awaryjnym najpierw probuje sie odzyskac; jesli sie nie uda, zapis jest pomijany
     * (dane zostaja w pamieci), zeby nie nadpisac prawdziwego rekordu pustym.
     */
    public CompletableFuture<Void> save(PlayerData playerData, String requesterIp) {
        long startNanos = System.nanoTime();
        return CompletableFuture.runAsync(() -> {
            if (playerData.isDegraded()) {
                try {
                    recover(playerData);
                } catch (Exception ex) {
                    totalErrors.incrementAndGet();
                    logger.warning("[DynamoSync] Zapis " + playerData.getUsername()
                            + " wstrzymany - baza niedostepna, profil w trybie awaryjnym (dane zostaja w pamieci).");
                    return;
                }
            }
            if (playerData.getCustomData().size() > InputValidator.MAX_CUSTOM_FIELDS) {
                totalWritesRejectedInvalid.incrementAndGet();
                logger.warning("[DynamoSync] Zapis " + playerData.getUsername() + " odrzucony - za duzo pol custom ("
                        + playerData.getCustomData().size() + " > " + InputValidator.MAX_CUSTOM_FIELDS + ").");
                return;
            }
            int estimatedSize = estimateItemSizeBytes(playerData);
            if (estimatedSize > MAX_ITEM_SIZE_BYTES) {
                totalWritesRejectedInvalid.incrementAndGet();
                logger.warning("[DynamoSync] Zapis " + playerData.getUsername() + " odrzucony - item za duzy (~"
                        + estimatedSize + " B > limit " + MAX_ITEM_SIZE_BYTES + " B). Sprawdz pola custom.");
                return;
            }
            IpRateLimiter limiter = rateLimiter;
            if (limiter != null) {
                limiter.record(requesterIp);
            }
            // dirty zdejmujemy PRZED migawka - zmiana w trakcie zapisu zostanie zapisana nastepnym razem.
            Map<String, AttributeValue> item = playerData.snapshotForWrite();
            try {
                client.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .build());
                totalWritesSent.incrementAndGet();
                if (debug) {
                    long ms = (System.nanoTime() - startNanos) / 1_000_000;
                    logger.info("[DynamoSync] [debug] Zapisano gracza " + playerData.getUsername()
                            + " w " + ms + " ms (WCU zuzyte, ~" + estimatedSize + " B)");
                }
            } catch (Exception ex) {
                playerData.markDirty(); // nie udalo sie - zostaje do ponowienia
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
            recoverDegradedEntries(); // po awarii bazy dolacza prawdziwe dane, zanim cokolwiek zapiszemy

            List<PlayerData> dirty = new ArrayList<>();
            for (PlayerData pd : cache.values()) {
                if (pd.isDirty() && !pd.isDegraded()) {
                    dirty.add(pd);
                }
            }
            totalAutosaveRuns.incrementAndGet();

            int saved = dirty.isEmpty() ? 0 : batchWriteAll(dirty);
            evictSettledOffline();

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

    /**
     * Dzieli liste na paczki po BATCH_WRITE_LIMIT i wysyla BatchWriteItem z retry na unprocessed items.
     * Kazdy item jest najpierw sprawdzany guardem rozmiaru - przewymiarowane rekordy sa pomijane
     * (nie blokuja calej paczki), zeby jeden zepsuty/za duzy profil nie zatrzymywal autosave dla wszystkich.
     * Jako zapisane liczy sie tylko to, czego baza NIE zwrocila w unprocessedItems i co nie padlo na bledzie -
     * reszta wraca do stanu dirty.
     */
    private int batchWriteAll(List<PlayerData> players) {
        int savedCount = 0;
        for (int i = 0; i < players.size(); i += BATCH_WRITE_LIMIT) {
            List<PlayerData> chunk = new ArrayList<>(players.subList(i, Math.min(i + BATCH_WRITE_LIMIT, players.size())));

            List<WriteRequest> requests = new ArrayList<>(chunk.size());
            Map<String, PlayerData> byUuid = new HashMap<>();
            for (PlayerData pd : chunk) {
                int size = estimateItemSizeBytes(pd);
                if (pd.getCustomData().size() > InputValidator.MAX_CUSTOM_FIELDS || size > MAX_ITEM_SIZE_BYTES) {
                    totalWritesRejectedInvalid.incrementAndGet();
                    logger.warning("[DynamoSync] Pomijam w autosave " + pd.getUsername() + " - item za duzy/niepoprawny (~"
                            + size + " B).");
                    continue;
                }
                // Zapis wsadowy (autosave/shutdown) zaliczamy do limitu IP gracza, ale go nie blokujemy.
                IpRateLimiter limiter = rateLimiter;
                if (limiter != null) {
                    limiter.record(pd.getSessionIp());
                }
                Map<String, AttributeValue> item = pd.snapshotForWrite(); // zdejmuje dirty przed zapisem
                requests.add(WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(item).build())
                        .build());
                byUuid.put(PlayerData.uuidWithoutDashes(pd.getUuid()), pd);
            }
            if (requests.isEmpty()) {
                continue;
            }

            Map<String, List<WriteRequest>> toSend = new HashMap<>();
            toSend.put(tableName, requests);

            int attempt = 0;
            while (!toSend.isEmpty()) {
                try {
                    BatchWriteItemResponse resp = client.batchWriteItem(BatchWriteItemRequest.builder()
                            .requestItems(toSend)
                            .build());
                    Map<String, List<WriteRequest>> unprocessed = resp.unprocessedItems();
                    if (unprocessed == null || unprocessed.isEmpty()) {
                        toSend = Map.of();
                        break;
                    }
                    toSend = new HashMap<>(unprocessed);
                    attempt++;
                    if (attempt >= MAX_BATCH_RETRIES) {
                        break;
                    }
                    long backoffMs = 100L * (1L << attempt); // proste exponential backoff
                    if (debug) {
                        logger.info("[DynamoSync] [debug] BatchWriteItem: throttling, "
                                + toSend.getOrDefault(tableName, List.of()).size()
                                + " itemow do ponowienia (proba " + attempt + "), czekam " + backoffMs + " ms.");
                    }
                    Thread.sleep(backoffMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception ex) {
                    totalErrors.incrementAndGet();
                    logger.log(Level.WARNING, "[DynamoSync] Blad BatchWriteItem (proba " + attempt + ")", ex);
                    break;
                }
            }

            // Wszystko, co zostalo w toSend, NIE zostalo potwierdzone przez baze -> wraca do dirty.
            int failed = 0;
            for (WriteRequest wr : toSend.getOrDefault(tableName, List.of())) {
                Map<String, AttributeValue> failedItem = wr.putRequest() != null ? wr.putRequest().item() : null;
                AttributeValue idAttr = failedItem != null ? failedItem.get("uuid") : null;
                PlayerData pd = idAttr != null ? byUuid.get(idAttr.s()) : null;
                if (pd != null) {
                    pd.markDirty();
                    failed++;
                }
            }
            if (failed > 0) {
                logger.warning("[DynamoSync] BatchWriteItem: " + failed + " itemow nie udalo sie zapisac"
                        + " (throttling/blad) - zostaja do ponowienia przy nastepnym autosave.");
            }
            int ok = requests.size() - failed;
            savedCount += ok;
            totalWritesSent.addAndGet(ok);
        }
        return savedCount;
    }

    // ------------------------------------------------------------------------------------------
    // Cache
    // ------------------------------------------------------------------------------------------

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
     * Usuwa wpis z cache TYLKO jesli to ten sam obiekt, gracz jest offline i dane sa zapisane (nie dirty, nie
     * awaryjne). Nieudany zapis przy wyjsciu zostawia wpis w pamieci do ponowienia przez autosave -
     * dane nie ginace razem z wyjsciem gracza.
     */
    public void evictIfSettled(PlayerData pd) {
        if (pd.isOffline() && !pd.isDirty() && !pd.isDegraded() && cache.remove(pd.getUuid(), pd) && debug) {
            logger.info("[DynamoSync] [debug] Usunieto z cache " + pd.getUuid() + " (cache size=" + cache.size() + ")");
        }
    }

    private void evictSettledOffline() {
        for (PlayerData pd : cache.values()) {
            evictIfSettled(pd);
        }
    }

    /**
     * Blokujacy zapis wszystkich BRUDNYCH graczy w cache - uzywane wylacznie w onDisable.
     * Uzywa BatchWriteItem tak samo jak autosave, zeby zamkniecie serwera bylo szybkie
     * nawet z wieloma graczami online.
     */
    public void saveAllBlocking() {
        recoverDegradedEntries();
        List<PlayerData> dirty = new ArrayList<>();
        for (PlayerData pd : cache.values()) {
            if (pd.isDegraded()) {
                logger.severe("[DynamoSync] Shutdown: profil " + pd.getUsername() + " nadal w trybie awaryjnym - "
                        + "NIE zapisuje go, zeby nie nadpisac prawdziwych danych w bazie. Zmiany z tej sesji przepadna.");
            } else if (pd.isDirty()) {
                dirty.add(pd);
            }
        }
        if (dirty.isEmpty()) {
            logger.info("[DynamoSync] Shutdown: brak zmian do zapisania (0 WCU zuzyte).");
            return;
        }
        int saved = batchWriteAll(dirty);
        logger.info("[DynamoSync] Shutdown: zapisano " + saved + "/" + dirty.size() + " brudnych graczy.");
    }

    /** Widok cache TYLKO DO ODCZYTU. */
    public Map<UUID, PlayerData> getCacheSnapshot() {
        return Collections.unmodifiableMap(cache);
    }

    public int getCacheSize() {
        return cache.size();
    }

    /** Rozmiar itemu w bajtach UTF-8 (z zapasem na narzut) - twardy guard PRZED wyslaniem do AWS. */
    private int estimateItemSizeBytes(PlayerData pd) {
        // stale pola (uuid, daty) + po ~32 B na kazda statystyke z StatType (nazwa + liczba + narzut atrybutu)
        int size = 128 + StatType.values().length * 32;
        size += utf8Length(pd.getUsername());
        for (String cause : pd.getDeathCauses().keySet()) {
            size += utf8Length(cause) + 16; // nazwa przyczyny + liczba + narzut
        }
        for (Map.Entry<String, String> e : pd.getCustomData().entrySet()) {
            size += utf8Length(e.getKey()) + utf8Length(e.getValue()) + 8; // +8: narzut atrybutu mapy
        }
        return size;
    }

    private static int utf8Length(String s) {
        return s == null ? 0 : s.getBytes(StandardCharsets.UTF_8).length;
    }

    /** Migawka statystyk do celow diagnostycznych (komenda /playerdata debug, logi). */
    public DebugStats getDebugStats() {
        return new DebugStats(
                cache.size(),
                totalLoadsFromDb.get(),
                totalLoadsFromCache.get(),
                totalWritesSent.get(),
                totalWritesSkippedClean.get(),
                totalWritesRejectedInvalid.get(),
                totalErrors.get(),
                totalAutosaveRuns.get(),
                lastAutosaveDurationMs,
                lastAutosaveDirtyCount,
                totalDegradedLoads.get(),
                totalRecovered.get()
        );
    }

    public record DebugStats(
            int cacheSize,
            long totalLoadsFromDb,
            long totalLoadsFromCache,
            long totalWritesSent,
            long totalWritesSkippedClean,
            long totalWritesRejectedInvalid,
            long totalErrors,
            long totalAutosaveRuns,
            long lastAutosaveDurationMs,
            long lastAutosaveDirtyCount,
            long totalDegradedLoads,
            long totalRecovered
    ) {}
}

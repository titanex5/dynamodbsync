package pl.twojserwer.dynamosync;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Warstwa dostepu do DynamoDB. Wszystkie wywolania sieciowe leca na osobnej puli
 * watkow (executor), nigdy na glownym watku serwera - inaczej kazdy lag AWS/OVH
 * bedzie zamrazal caly serwer.
 */
public class PlayerDataManager {

    private final DynamoDbClient client;
    private final String tableName;
    private final ExecutorService executor;
    private final Logger logger;
    private final boolean debug;

    private final Map<UUID, PlayerData> cache = new ConcurrentHashMap<>();

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
            return CompletableFuture.completedFuture(cached);
        }
        return CompletableFuture.supplyAsync(() -> {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("uuid", AttributeValue.builder().s(uuid.toString()).build());
            GetItemResponse resp = client.getItem(GetItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .build());
            PlayerData playerData = PlayerData.fromItem(uuid, usernameHint, resp.item());
            cache.put(uuid, playerData);
            if (debug) {
                logger.info("[DynamoSync] Zaladowano dane gracza " + usernameHint + " (" + uuid + ")");
            }
            return playerData;
        }, executor).exceptionally(ex -> {
            logger.log(Level.WARNING, "[DynamoSync] Blad ladowania danych gracza " + uuid, ex);
            PlayerData fallback = new PlayerData(uuid, usernameHint);
            cache.put(uuid, fallback);
            return fallback;
        });
    }

    /** Zapisuje pojedynczego gracza do DynamoDB asynchronicznie. */
    public CompletableFuture<Void> save(PlayerData playerData) {
        return CompletableFuture.runAsync(() -> {
            try {
                client.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(playerData.toItem())
                        .build());
                if (debug) {
                    logger.info("[DynamoSync] Zapisano dane gracza " + playerData.getUsername());
                }
            } catch (Exception ex) {
                logger.log(Level.WARNING, "[DynamoSync] Blad zapisu danych gracza " + playerData.getUsername(), ex);
            }
        }, executor);
    }

    /** Zwraca dane gracza z cache (bez zapytania do sieci) - null jesli jeszcze nie zaladowane. */
    public PlayerData getCached(UUID uuid) {
        return cache.get(uuid);
    }

    public void evictFromCache(UUID uuid) {
        cache.remove(uuid);
    }

    /** Blokujacy zapis wszystkich graczy w cache - uzywane wylacznie w onDisable. */
    public void saveAllBlocking() {
        for (PlayerData pd : cache.values()) {
            try {
                client.putItem(PutItemRequest.builder()
                        .tableName(tableName)
                        .item(pd.toItem())
                        .build());
            } catch (Exception ex) {
                logger.log(Level.WARNING, "[DynamoSync] Blad zapisu (shutdown) gracza " + pd.getUsername(), ex);
            }
        }
    }

    public Map<UUID, PlayerData> getCacheSnapshot() {
        return cache;
    }
}

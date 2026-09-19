package pl.twojserwer.dynamosync;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reprezentuje dane pojedynczego gracza trzymane w DynamoDB.
 * Pole "data" to dowolna mapa string->string na potrzeby innych pluginow/rozszerzen
 * (np. saldo ekonomii, rangi, statystyki itd.).
 */
public class PlayerData {

    private final UUID uuid;
    private String username;
    private long firstJoin;
    private long lastJoin;
    private long playtimeSeconds;
    private final Map<String, String> data;

    public PlayerData(UUID uuid, String username) {
        this.uuid = uuid;
        this.username = username;
        this.firstJoin = System.currentTimeMillis();
        this.lastJoin = this.firstJoin;
        this.playtimeSeconds = 0L;
        this.data = new ConcurrentHashMap<>();
    }

    private PlayerData(UUID uuid, String username, long firstJoin, long lastJoin,
                        long playtimeSeconds, Map<String, String> data) {
        this.uuid = uuid;
        this.username = username;
        this.firstJoin = firstJoin;
        this.lastJoin = lastJoin;
        this.playtimeSeconds = playtimeSeconds;
        this.data = new ConcurrentHashMap<>(data);
    }

    public UUID getUuid() {
        return uuid;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public long getFirstJoin() {
        return firstJoin;
    }

    public long getLastJoin() {
        return lastJoin;
    }

    public void setLastJoin(long lastJoin) {
        this.lastJoin = lastJoin;
    }

    public long getPlaytimeSeconds() {
        return playtimeSeconds;
    }

    public void addPlaytimeSeconds(long seconds) {
        this.playtimeSeconds += Math.max(0, seconds);
    }

    public String getCustom(String key) {
        return data.get(key);
    }

    public void setCustom(String key, String value) {
        data.put(key, value);
    }

    public Map<String, String> getCustomData() {
        return data;
    }

    /** Konwersja do formatu wymaganego przez PutItemRequest. */
    public Map<String, AttributeValue> toItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("uuid", AttributeValue.builder().s(uuid.toString()).build());
        item.put("username", AttributeValue.builder().s(username == null ? "" : username).build());
        item.put("firstJoin", AttributeValue.builder().n(Long.toString(firstJoin)).build());
        item.put("lastJoin", AttributeValue.builder().n(Long.toString(lastJoin)).build());
        item.put("playtimeSeconds", AttributeValue.builder().n(Long.toString(playtimeSeconds)).build());

        Map<String, AttributeValue> dataMap = new HashMap<>();
        for (Map.Entry<String, String> e : data.entrySet()) {
            dataMap.put(e.getKey(), AttributeValue.builder().s(e.getValue()).build());
        }
        item.put("data", AttributeValue.builder().m(dataMap).build());
        return item;
    }

    /** Odtworzenie obiektu z GetItemResponse. */
    public static PlayerData fromItem(UUID uuid, String fallbackUsername, Map<String, AttributeValue> item) {
        if (item == null || item.isEmpty()) {
            return new PlayerData(uuid, fallbackUsername);
        }
        String username = item.containsKey("username") ? item.get("username").s() : fallbackUsername;
        long firstJoin = item.containsKey("firstJoin") ? Long.parseLong(item.get("firstJoin").n()) : System.currentTimeMillis();
        long lastJoin = item.containsKey("lastJoin") ? Long.parseLong(item.get("lastJoin").n()) : firstJoin;
        long playtime = item.containsKey("playtimeSeconds") ? Long.parseLong(item.get("playtimeSeconds").n()) : 0L;

        Map<String, String> data = new HashMap<>();
        if (item.containsKey("data") && item.get("data").hasM()) {
            for (Map.Entry<String, AttributeValue> e : item.get("data").m().entrySet()) {
                data.put(e.getKey(), e.getValue().s());
            }
        }
        return new PlayerData(uuid, username, firstJoin, lastJoin, playtime, data);
    }
}

package pl.twojserwer.dynamosync;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Reprezentuje profil pojedynczego gracza trzymany w tabeli DynamoDB "Profiles".
 *
 * Schemat rekordu:
 *  - uuid          (partition key) - UUID gracza BEZ myslnikow (32 znaki hex)
 *  - username      - aktualny nick
 *  - createdDate    - data utworzenia profilu, format dd-MM-yyyy HH:mm:ss
 *  - lastSeen      - data ostatniej aktywnosci, format dd-MM-yyyy HH:mm:ss
 *  - playtime      - sekundy spedzone na serwerze (liczba)
 *  - kills         - zabojstwa PvP (liczba)
 *  - deaths        - smierci (liczba)
 *  - data          - mapa string->string na dowolne dodatkowe statystyki/rozszerzenia
 *
 * Sledzi flage "dirty" - zapis do DynamoDB (PutItem/BatchWriteItem) kosztuje
 * WCU proporcjonalnie do rozmiaru itemu niezaleznie od tego, czy cokolwiek
 * naprawde sie zmienilo. Zapisujac tylko "brudnych" graczy przy autosave
 * ograniczamy liczbe platnych operacji zapisu do minimum.
 */
public class PlayerData {

    /** Wspolny format daty uzywany dla createdDate/lastSeen w calym pluginie. */
    public static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss").withZone(ZoneId.systemDefault());

    private final UUID uuid;
    private String username;
    private long createdDateMillis;
    private long lastSeenMillis;
    private long playtime;
    private long kills;
    private long deaths;
    private final Map<String, String> data;
    private final AtomicBoolean dirty = new AtomicBoolean(false);

    public PlayerData(UUID uuid, String username) {
        this.uuid = uuid;
        this.username = username;
        this.createdDateMillis = System.currentTimeMillis();
        this.lastSeenMillis = this.createdDateMillis;
        this.playtime = 0L;
        this.kills = 0L;
        this.deaths = 0L;
        this.data = new ConcurrentHashMap<>();
        // Nowy rekord (lub fallback po bledzie odczytu) - chcemy go zapisac
        // przy najblizszej okazji, wiec od razu oznaczamy jako dirty.
        this.dirty.set(true);
    }

    private PlayerData(UUID uuid, String username, long createdDateMillis, long lastSeenMillis,
                        long playtime, long kills, long deaths, Map<String, String> data) {
        this.uuid = uuid;
        this.username = username;
        this.createdDateMillis = createdDateMillis;
        this.lastSeenMillis = lastSeenMillis;
        this.playtime = playtime;
        this.kills = kills;
        this.deaths = deaths;
        this.data = new ConcurrentHashMap<>(data);
        // Swiezo zaladowane z DynamoDB - zgodne ze stanem w bazie, wiec czyste.
        this.dirty.set(false);
    }

    /** Czy dane zmienily sie od ostatniego zapisu do DynamoDB. */
    public boolean isDirty() {
        return dirty.get();
    }

    /** Wywolywane po udanym zapisie do DynamoDB. */
    public void clearDirty() {
        dirty.set(false);
    }

    public void markDirty() {
        dirty.set(true);
    }

    public UUID getUuid() {
        return uuid;
    }

    /** UUID w formacie zapisywanym w DynamoDB - 32 znaki hex, bez myslnikow. */
    public static String uuidWithoutDashes(UUID uuid) {
        return uuid.toString().replace("-", "");
    }

    /** Odtwarza java.util.UUID z formatu bez myslnikow (dodaje je z powrotem w standardowych pozycjach). */
    public static UUID uuidFromDashless(String dashless) {
        if (dashless == null || dashless.length() != 32) {
            throw new IllegalArgumentException("Nieprawidlowy format UUID (oczekiwano 32 znakow hex): " + dashless);
        }
        String withDashes = dashless.substring(0, 8) + "-" + dashless.substring(8, 12) + "-"
                + dashless.substring(12, 16) + "-" + dashless.substring(16, 20) + "-" + dashless.substring(20, 32);
        return UUID.fromString(withDashes);
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        if (username != null && !username.equals(this.username)) {
            this.username = username;
            markDirty();
        }
    }

    public long getCreatedDateMillis() {
        return createdDateMillis;
    }

    public String getCreatedDateFormatted() {
        return DATE_FORMAT.format(Instant.ofEpochMilli(createdDateMillis));
    }

    public long getLastSeenMillis() {
        return lastSeenMillis;
    }

    public String getLastSeenFormatted() {
        return DATE_FORMAT.format(Instant.ofEpochMilli(lastSeenMillis));
    }

    /** Aktualizuje lastSeen do "teraz". Wywolywane przy kazdym wejsciu gracza. */
    public void touchLastSeen() {
        this.lastSeenMillis = System.currentTimeMillis();
        markDirty();
    }

    public long getPlaytime() {
        return playtime;
    }

    public void addPlaytimeSeconds(long seconds) {
        if (seconds <= 0) {
            return;
        }
        this.playtime += seconds;
        markDirty();
    }

    public long getKills() {
        return kills;
    }

    public void addKill() {
        this.kills++;
        markDirty();
    }

    public long getDeaths() {
        return deaths;
    }

    public void addDeath() {
        this.deaths++;
        markDirty();
    }

    /** Wspolczynnik kills/deaths - deaths=0 traktowane jako 1, zeby uniknac dzielenia przez zero. */
    public double getKdRatio() {
        return (double) kills / Math.max(1L, deaths);
    }

    public String getCustom(String key) {
        return data.get(key);
    }

    /**
     * Ustawia dodatkowe pole custom. UWAGA: ta metoda NIE waliduje wejscia -
     * walidacja (dlugosc, dozwolone znaki, zarezerwowane nazwy) odbywa sie
     * w PlayerDataManager.validateCustomField(), zanim dane w ogole tu trafia.
     * Wolne wywolanie tej metody z nieznanego/niezaufanego zrodla nalezy
     * zawsze poprzedzic ta walidacja.
     */
    public void setCustom(String key, String value) {
        String previous = data.put(key, value);
        if (!java.util.Objects.equals(previous, value)) {
            markDirty();
        }
    }

    public Map<String, String> getCustomData() {
        return data;
    }

    /** Konwersja do formatu wymaganego przez PutItemRequest (tabela "Profiles"). */
    public Map<String, AttributeValue> toItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("uuid", AttributeValue.builder().s(uuidWithoutDashes(uuid)).build());
        item.put("username", AttributeValue.builder().s(username == null ? "" : username).build());
        item.put("createdDate", AttributeValue.builder().s(getCreatedDateFormatted()).build());
        item.put("lastSeen", AttributeValue.builder().s(getLastSeenFormatted()).build());
        item.put("playtime", AttributeValue.builder().n(Long.toString(playtime)).build());
        item.put("kills", AttributeValue.builder().n(Long.toString(kills)).build());
        item.put("deaths", AttributeValue.builder().n(Long.toString(deaths)).build());

        Map<String, AttributeValue> dataMap = new HashMap<>();
        for (Map.Entry<String, String> e : data.entrySet()) {
            dataMap.put(e.getKey(), AttributeValue.builder().s(e.getValue()).build());
        }
        item.put("data", AttributeValue.builder().m(dataMap).build());
        return item;
    }

    /** Odtworzenie obiektu z GetItemResponse. Odporne na braki/zle typy pol (np. po recznej edycji w AWS Console). */
    public static PlayerData fromItem(UUID uuid, String fallbackUsername, Map<String, AttributeValue> item) {
        if (item == null || item.isEmpty()) {
            return new PlayerData(uuid, fallbackUsername);
        }
        String username = item.containsKey("username") ? item.get("username").s() : fallbackUsername;

        long createdDateMillis = parseDateOrNow(item.get("createdDate"), System.currentTimeMillis());
        long lastSeenMillis = parseDateOrNow(item.get("lastSeen"), createdDateMillis);

        long playtime = parseLongOrDefault(item.get("playtime"), 0L);
        long kills = parseLongOrDefault(item.get("kills"), 0L);
        long deaths = parseLongOrDefault(item.get("deaths"), 0L);

        Map<String, String> data = new HashMap<>();
        if (item.containsKey("data") && item.get("data").hasM()) {
            for (Map.Entry<String, AttributeValue> e : item.get("data").m().entrySet()) {
                data.put(e.getKey(), e.getValue().s());
            }
        }
        return new PlayerData(uuid, username, createdDateMillis, lastSeenMillis, playtime, kills, deaths, data);
    }

    private static long parseDateOrNow(AttributeValue value, long fallback) {
        if (value == null || value.s() == null) {
            return fallback;
        }
        try {
            return java.time.LocalDateTime.parse(value.s(), DATE_FORMAT)
                    .atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
        } catch (Exception ex) {
            // Rekord uszkodzony/edytowany recznie w AWS Console w innym formacie - nie wywalamy calego load()
            return fallback;
        }
    }

    private static long parseLongOrDefault(AttributeValue value, long fallback) {
        if (value == null || value.n() == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value.n());
        } catch (NumberFormatException ex) {
            return fallback;
        }
    }
}

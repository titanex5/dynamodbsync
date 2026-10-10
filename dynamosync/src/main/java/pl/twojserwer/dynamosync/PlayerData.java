package pl.twojserwer.dynamosync;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

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
 *
 * Twardnienie (v1.1.0):
 *  - tryb awaryjny ("degraded"): gdy odczyt z bazy sie nie powiedzie, gracz dostaje TYMCZASOWY
 *    pusty profil, ktory NIGDY nie jest zapisywany do bazy, dopoki nie uda sie odczytac prawdziwego
 *    rekordu i polaczyc z nim zmian z tej sesji (zob. {@link #recoverFrom(PlayerData)}).
 *    Wczesniej pusty profil zastepowal w bazie prawdziwy - wystarczyl chwilowy throttling/awaria,
 *    zeby zresetowac graczowi statystyki,
 *  - pola custom sa walidowane takze w {@link #setCustom} (nie tylko w komendzie),
 *  - dane odczytane z bazy sa traktowane jako niezaufane (brak NPE na zlym typie pola, liczby >= 0),
 *  - liczniki sa chronione przed wyscigiem miedzy glownym watkiem a watkami zapisu.
 */
public class PlayerData {

    /** Wspolny format daty uzywany dla createdDate/lastSeen w calym pluginie. */
    public static final DateTimeFormatter DATE_FORMAT =
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss").withZone(ZoneId.systemDefault());

    private static final Pattern DASHLESS_UUID = Pattern.compile("^[0-9a-fA-F]{32}$");

    private final UUID uuid;
    private volatile String username;
    private volatile long createdDateMillis;
    private volatile long lastSeenMillis;
    // playtime/kills/deaths: dostep tylko przez metody synchronized (main thread vs watki zapisu).
    private long playtime;
    private long kills;
    private long deaths;
    private final Map<String, String> data;
    private final AtomicBoolean dirty = new AtomicBoolean(false);

    /**
     * Adres IP, z ktorego gracz jest aktualnie polaczony. Tylko w pamieci (NIE trafia do DynamoDB) -
     * sluzy do przypisania zapisow (np. autosave) do limitu zapytan danego IP.
     */
    private volatile String sessionIp;

    /** Tymczasowy profil po bledzie odczytu - nie wolno go zapisywac do bazy (zob. opis klasy). */
    private volatile boolean degraded;

    /** Gracz wyszedl - wpis mozna usunac z cache, gdy tylko jest zapisany. */
    private volatile boolean offline;

    public PlayerData(UUID uuid, String username) {
        this.uuid = uuid;
        this.username = InputValidator.sanitizeUsername(username);
        this.createdDateMillis = System.currentTimeMillis();
        this.lastSeenMillis = this.createdDateMillis;
        this.playtime = 0L;
        this.kills = 0L;
        this.deaths = 0L;
        this.data = new ConcurrentHashMap<>();
        // Nowy rekord - chcemy go zapisac przy najblizszej okazji, wiec od razu oznaczamy jako dirty.
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

    /**
     * Tymczasowy profil na czas awarii odczytu. Zbiera zmiany z sesji (kills, deaths, playtime, pola custom)
     * jako "delty", ale jest ignorowany przez wszystkie sciezki zapisu, dopoki nie zostanie odzyskany.
     */
    public static PlayerData degradedFallback(UUID uuid, String username) {
        PlayerData pd = new PlayerData(uuid, username);
        pd.degraded = true;
        return pd;
    }

    /** Czy to tymczasowy profil awaryjny (nie wolno go zapisywac). */
    public boolean isDegraded() {
        return degraded;
    }

    /**
     * Laczy prawdziwy rekord z bazy z zmianami zebranymi w trybie awaryjnym i wychodzi z trybu awaryjnego.
     * Zmienia ten sam obiekt (a nie podmienia go w cache), wiec nikt, kto trzyma referencje, nie zgubi zmian.
     * Idempotentne - drugie wywolanie nic nie robi (chroni przed podwojnym doliczeniem statystyk).
     *
     * @return true jesli faktycznie odzyskano, false jesli juz wczesniej odzyskany
     */
    public synchronized boolean recoverFrom(PlayerData persisted) {
        if (!degraded) {
            return false;
        }
        this.kills += persisted.getKills();
        this.deaths += persisted.getDeaths();
        this.playtime += persisted.getPlaytime();
        this.createdDateMillis = Math.min(this.createdDateMillis, persisted.createdDateMillis);
        this.lastSeenMillis = Math.max(this.lastSeenMillis, persisted.lastSeenMillis);
        // Pola custom: wartosci z tej sesji maja pierwszenstwo, reszta z bazy zostaje.
        for (Map.Entry<String, String> e : persisted.data.entrySet()) {
            this.data.putIfAbsent(e.getKey(), e.getValue());
        }
        this.degraded = false;
        markDirty();
        return true;
    }

    public boolean isOffline() {
        return offline;
    }

    public void markOffline() {
        this.offline = true;
    }

    public void markOnline() {
        this.offline = false;
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

    /**
     * Zdejmuje flage dirty i JEDNOCZESNIE robi migawke do zapisu. Kolejnosc ma znaczenie: zmiana, ktora
     * wpadnie miedzy migawka a koniec zapisu, ustawi dirty od nowa i zostanie zapisana nastepnym razem
     * (wczesniej clearDirty() po zapisie potrafilo "zjesc" taka zmiane). Gdy zapis sie nie uda,
     * wolajacy musi zrobic {@link #markDirty()}.
     */
    public Map<String, AttributeValue> snapshotForWrite() {
        dirty.set(false);
        return toItem();
    }

    public UUID getUuid() {
        return uuid;
    }

    public String getSessionIp() {
        return sessionIp;
    }

    public void setSessionIp(String sessionIp) {
        this.sessionIp = sessionIp;
    }

    /** UUID w formacie zapisywanym w DynamoDB - 32 znaki hex, bez myslnikow. */
    public static String uuidWithoutDashes(UUID uuid) {
        return uuid.toString().replace("-", "");
    }

    /** Odtwarza java.util.UUID z formatu bez myslnikow (dodaje je z powrotem w standardowych pozycjach). */
    public static UUID uuidFromDashless(String dashless) {
        if (dashless == null || !DASHLESS_UUID.matcher(dashless).matches()) {
            throw new IllegalArgumentException("Nieprawidlowy format UUID (oczekiwano 32 znakow hex)");
        }
        String withDashes = dashless.substring(0, 8) + "-" + dashless.substring(8, 12) + "-"
                + dashless.substring(12, 16) + "-" + dashless.substring(16, 20) + "-" + dashless.substring(20, 32);
        return UUID.fromString(withDashes);
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        String clean = InputValidator.sanitizeUsername(username);
        if (!clean.isEmpty() && !clean.equals(this.username)) {
            this.username = clean;
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

    public synchronized long getPlaytime() {
        return playtime;
    }

    public synchronized void addPlaytimeSeconds(long seconds) {
        if (seconds <= 0) {
            return;
        }
        this.playtime += seconds;
        markDirty();
    }

    public synchronized long getKills() {
        return kills;
    }

    public synchronized void addKill() {
        this.kills++;
        markDirty();
    }

    public synchronized long getDeaths() {
        return deaths;
    }

    public synchronized void addDeath() {
        this.deaths++;
        markDirty();
    }

    /** Wspolczynnik kills/deaths - deaths=0 traktowane jako 1, zeby uniknac dzielenia przez zero. */
    public double getKdRatio() {
        long k;
        long d;
        synchronized (this) {
            k = kills;
            d = deaths;
        }
        return (double) k / Math.max(1L, d);
    }

    public String getCustom(String key) {
        return data.get(key);
    }

    /**
     * Ustawia dodatkowe pole custom. Wejscie jest WALIDOWANE ({@link InputValidator}): klucz, dlugosc wartosci,
     * znaki kontrolne, zarezerwowane nazwy i maksymalna liczba pol. Dzieki temu inny plugin korzystajacy
     * z API nie moze (przez blad albo celowo) zepsuc rekordu ani rozdac go ponad limit 400 KB.
     *
     * @throws IllegalArgumentException gdy klucz/wartosc nie przechodzi walidacji
     */
    public void setCustom(String key, String value) {
        String error = InputValidator.validateCustomField(key, value);
        if (error != null) {
            throw new IllegalArgumentException(error);
        }
        if (!data.containsKey(key) && data.size() >= InputValidator.MAX_CUSTOM_FIELDS) {
            throw new IllegalArgumentException("Za duzo pol custom (max " + InputValidator.MAX_CUSTOM_FIELDS + ").");
        }
        String previous = data.put(key, value);
        if (!java.util.Objects.equals(previous, value)) {
            markDirty();
        }
    }

    /** Widok TYLKO DO ODCZYTU - zmiany przechodza przez {@link #setCustom}, zeby nie omijac walidacji. */
    public Map<String, String> getCustomData() {
        return Collections.unmodifiableMap(data);
    }

    /** Konwersja do formatu wymaganego przez PutItemRequest (tabela "Profiles"). */
    public Map<String, AttributeValue> toItem() {
        long p;
        long k;
        long d;
        synchronized (this) {
            p = playtime;
            k = kills;
            d = deaths;
        }
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("uuid", AttributeValue.builder().s(uuidWithoutDashes(uuid)).build());
        item.put("username", AttributeValue.builder().s(username == null ? "" : username).build());
        item.put("createdDate", AttributeValue.builder().s(getCreatedDateFormatted()).build());
        item.put("lastSeen", AttributeValue.builder().s(getLastSeenFormatted()).build());
        item.put("playtime", AttributeValue.builder().n(Long.toString(p)).build());
        item.put("kills", AttributeValue.builder().n(Long.toString(k)).build());
        item.put("deaths", AttributeValue.builder().n(Long.toString(d)).build());

        Map<String, AttributeValue> dataMap = new HashMap<>();
        for (Map.Entry<String, String> e : data.entrySet()) {
            dataMap.put(e.getKey(), AttributeValue.builder().s(e.getValue()).build());
        }
        item.put("data", AttributeValue.builder().m(dataMap).build());
        return item;
    }

    /**
     * Odtworzenie obiektu z GetItemResponse. Rekord z bazy jest traktowany jako NIEZAUFANY (ktos mogl go
     * edytowac w AWS Console albo inny program): zly typ pola, brak pola, ujemne liczby czy dziwny nick
     * nie moga wywrocic odczytu (a tym bardziej doprowadzic do nadpisania rekordu pustym profilem).
     */
    public static PlayerData fromItem(UUID uuid, String fallbackUsername, Map<String, AttributeValue> item) {
        if (item == null || item.isEmpty()) {
            return new PlayerData(uuid, fallbackUsername);
        }
        AttributeValue storedName = item.get("username");
        String rawName = (storedName != null && storedName.s() != null) ? storedName.s() : fallbackUsername;
        String username = InputValidator.sanitizeUsername(rawName);
        if (username.isEmpty()) {
            username = InputValidator.sanitizeUsername(fallbackUsername);
        }

        long createdDateMillis = parseDateOrNow(item.get("createdDate"), System.currentTimeMillis());
        long lastSeenMillis = parseDateOrNow(item.get("lastSeen"), createdDateMillis);

        long playtime = Math.max(0L, parseLongOrDefault(item.get("playtime"), 0L));
        long kills = Math.max(0L, parseLongOrDefault(item.get("kills"), 0L));
        long deaths = Math.max(0L, parseLongOrDefault(item.get("deaths"), 0L));

        Map<String, String> data = new HashMap<>();
        AttributeValue dataAttr = item.get("data");
        if (dataAttr != null && dataAttr.hasM()) {
            for (Map.Entry<String, AttributeValue> e : dataAttr.m().entrySet()) {
                AttributeValue v = e.getValue();
                // Tylko wartosci tekstowe - cokolwiek innego pomijamy zamiast wywalac caly odczyt.
                if (e.getKey() != null && v != null && v.s() != null) {
                    data.put(e.getKey(), v.s());
                }
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

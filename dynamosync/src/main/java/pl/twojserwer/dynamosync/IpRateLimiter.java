package pl.twojserwer.dynamosync;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Limit zapytan do bazy danych per adres IP - "przesuwane okno" (sliding window).
 *
 * Domyslnie: maksymalnie {@code maxRequests} zapytan (np. 50) w ciagu
 * {@code windowSeconds} sekund (np. 300 = 5 minut) z tego samego adresu IP.
 * Dla kazdego IP trzymamy znaczniki czasu ostatnich zapytan; zapytanie jest
 * dozwolone, jesli w ostatnich {@code windowSeconds} sekundach bylo ich mniej
 * niz {@code maxRequests}. Przesuwane okno (a nie sztywne przedzialy 5-minutowe)
 * nie pozwala "oszukac" limitu przez 50 zapytan tuz przed i 50 tuz po
 * zmianie przedzialu.
 *
 * Dwa rodzaje wpisow:
 *  - {@link #tryAcquire(String)} - zapytanie, ktore MOZNA odrzucic (np. odczyt
 *    przy wejsciu gracza). Zuzywa limit tylko gdy sie zmiesci.
 *  - {@link #record(String)} - zapytanie, ktorego NIE WOLNO odrzucic (np. zapis
 *    danych gracza - odrzucenie oznaczaloby utrate danych). Zawsze zuzywa limit.
 *
 * Klasa jest watkowo bezpieczna - kazda operacja na wpisach danego IP jest
 * atomowa dzieki ConcurrentHashMap.compute().
 *
 * Twardnienie (v1.1.0):
 *  - IPv6 liczymy per podsiec /64, nie per pojedynczy adres. Atakujacy z IPv6 zwykle dostaje
 *    cala /64 (2^64 adresow) i bez tego mialby "nowy" limit przy kazdym polaczeniu.
 *    Zwykly gracz (jeden dom = jedna /64 lub szersza) nie widzi roznicy - tak samo jak
 *    graczy za jednym NAT-em IPv4.
 *  - Liczba sledzonych kluczy jest ograniczona ({@link #MAX_TRACKED_KEYS}), zeby botnet z miliona
 *    adresow nie zjadl pamieci serwera. Po przepelnieniu dzialamy "fail-open": NOWE adresy nie sa
 *    limitowane (lepiej przepuscic zapytanie niz odciac prawdziwych graczy).
 */
public final class IpRateLimiter {

    /** Maksymalna liczba jednoczesnie sledzonych adresow/podsieci. */
    public static final int MAX_TRACKED_KEYS = 50_000;

    private final int maxRequests;
    private final long windowNanos;
    private final long windowMillis;

    private final Map<String, ArrayDeque<Long>> hits = new ConcurrentHashMap<>();
    private final AtomicLong totalDenied = new AtomicLong();
    private final AtomicLong overflowSkips = new AtomicLong();
    private volatile long lastEmergencyCleanupNanos = System.nanoTime();

    public IpRateLimiter(int maxRequests, int windowSeconds) {
        if (maxRequests < 1) {
            throw new IllegalArgumentException("maxRequests musi byc >= 1");
        }
        if (windowSeconds < 1) {
            throw new IllegalArgumentException("windowSeconds musi byc >= 1");
        }
        this.maxRequests = maxRequests;
        this.windowMillis = TimeUnit.SECONDS.toMillis(windowSeconds);
        this.windowNanos = TimeUnit.SECONDS.toNanos(windowSeconds);
    }

    /**
     * Probuje zuzyc jedno zapytanie z limitu tego IP.
     * IP = null oznacza "brak adresu" (konsola, zadania wewnetrzne serwera) - bez limitu.
     *
     * @return true jesli zapytanie wolno wykonac, false jesli limit wyczerpany
     */
    public boolean tryAcquire(String ip) {
        if (ip == null) {
            return true;
        }
        ip = normalize(ip);
        if (tableFullFor(ip)) {
            return true; // fail-open - patrz opis klasy
        }
        boolean[] allowed = new boolean[1];
        hits.compute(ip, (k, queue) -> {
            long now = System.nanoTime();
            ArrayDeque<Long> q = queue != null ? queue : new ArrayDeque<>();
            prune(q, now);
            if (q.size() < maxRequests) {
                q.addLast(now);
                allowed[0] = true;
            }
            return q;
        });
        if (!allowed[0]) {
            totalDenied.incrementAndGet();
        }
        return allowed[0];
    }

    /**
     * Zalicza zapytanie do limitu BEZ mozliwosci odrzucenia (np. zapis danych gracza).
     * Dzieki temu zapisy tez "zjadaja" budzet IP, ale nigdy nie sa gubione.
     */
    public void record(String ip) {
        if (ip == null) {
            return;
        }
        ip = normalize(ip);
        if (tableFullFor(ip)) {
            return;
        }
        hits.compute(ip, (k, queue) -> {
            long now = System.nanoTime();
            ArrayDeque<Long> q = queue != null ? queue : new ArrayDeque<>();
            prune(q, now);
            // Kolejka nigdy nie rosnie ponad maxRequests*2 - zapisy MUSZA przejsc, ale pamiec jest ograniczona.
            if (q.size() >= maxRequests * 2) {
                q.pollFirst();
            }
            q.addLast(now);
            return q;
        });
    }

    /**
     * Ile milisekund trzeba poczekac, az z tego IP bedzie mozna wykonac kolejne zapytanie.
     * 0 = mozna od razu. Nie zuzywa limitu (sluzy do wczesnego odrzucenia przy logowaniu
     * i do komunikatow "sprobuj za X s").
     */
    public long retryAfterMillis(String ip) {
        if (ip == null) {
            return 0L;
        }
        long[] wait = new long[1];
        hits.computeIfPresent(normalize(ip), (k, q) -> {
            long now = System.nanoTime();
            prune(q, now);
            if (q.size() >= maxRequests) {
                // Wolne miejsce zrobi sie, gdy najstarszy wpis wypadnie z okna.
                long oldest = q.peekFirst();
                wait[0] = Math.max(1L, TimeUnit.NANOSECONDS.toMillis(oldest + windowNanos - now));
            }
            return q.isEmpty() ? null : q;
        });
        return wait[0];
    }

    /** Ile zapytan z tego IP zostalo jeszcze w biezacym oknie. */
    public int remaining(String ip) {
        if (ip == null) {
            return Integer.MAX_VALUE;
        }
        int[] used = new int[1];
        hits.computeIfPresent(normalize(ip), (k, q) -> {
            prune(q, System.nanoTime());
            used[0] = q.size();
            return q.isEmpty() ? null : q;
        });
        return Math.max(0, maxRequests - used[0]);
    }

    /** Usuwa wygasle wpisy i puste adresy IP, zeby mapa nie rosla w nieskonczonosc. */
    public void cleanup() {
        for (String ip : hits.keySet()) {
            hits.computeIfPresent(ip, (k, q) -> {
                prune(q, System.nanoTime());
                return q.isEmpty() ? null : q;
            });
        }
    }

    /**
     * Sprowadza adres do klucza limitu: IPv4 bez zmian, IPv6 do prefiksu /64.
     * Przyjmuje tekst z {@code InetAddress#getHostAddress()}; przy czymkolwiek niezrozumialym
     * zwraca wejscie bez zmian (nigdy nie rzuca wyjatku).
     */
    public static String normalize(String ip) {
        if (ip == null || ip.indexOf(':') < 0) {
            return ip;
        }
        try {
            int scope = ip.indexOf('%');
            String literal = scope >= 0 ? ip.substring(0, scope) : ip;
            // Literal zawierajacy ':' jest parsowany lokalnie - bez zapytania DNS.
            InetAddress addr = InetAddress.getByName(literal);
            if (addr instanceof Inet6Address) {
                byte[] b = addr.getAddress();
                StringBuilder sb = new StringBuilder("v6:");
                for (int i = 0; i < 8; i++) {
                    sb.append(String.format("%02x", b[i]));
                }
                return sb.append("/64").toString();
            }
            return addr.getHostAddress(); // np. IPv4 w mapowaniu ::ffff:a.b.c.d
        } catch (Exception ex) {
            return ip;
        }
    }

    /**
     * Czy tablica jest przepelniona i {@code key} jest nowy (wtedy go nie sledzimy).
     * Przy przepelnieniu probuje awaryjnie posprzatac (najwyzej raz na 5 s, zeby atak nie
     * zamienil kazdego zapytania w pelny skan mapy).
     */
    private boolean tableFullFor(String key) {
        if (hits.size() < MAX_TRACKED_KEYS || hits.containsKey(key)) {
            return false;
        }
        long now = System.nanoTime();
        if (now - lastEmergencyCleanupNanos > TimeUnit.SECONDS.toNanos(5)) {
            lastEmergencyCleanupNanos = now;
            cleanup();
        }
        if (hits.size() >= MAX_TRACKED_KEYS) {
            overflowSkips.incrementAndGet();
            return true;
        }
        return false;
    }

    /** Ile razy pominieto sledzenie nowego adresu, bo tablica byla pelna (sygnal trwajacego ataku). */
    public long getOverflowSkips() {
        return overflowSkips.get();
    }

    public int trackedIpCount() {
        return hits.size();
    }

    public long getTotalDenied() {
        return totalDenied.get();
    }

    public int getMaxRequests() {
        return maxRequests;
    }

    public long getWindowSeconds() {
        return TimeUnit.MILLISECONDS.toSeconds(windowMillis);
    }

    private void prune(ArrayDeque<Long> q, long now) {
        // Znaczniki sa dodawane rosnaco, wiec wystarczy zdejmowac z poczatku.
        while (!q.isEmpty() && now - q.peekFirst() >= windowNanos) {
            q.pollFirst();
        }
    }
}

package pl.twojserwer.dynamosync;

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
 */
public final class IpRateLimiter {

    private final int maxRequests;
    private final long windowNanos;
    private final long windowMillis;

    private final Map<String, ArrayDeque<Long>> hits = new ConcurrentHashMap<>();
    private final AtomicLong totalDenied = new AtomicLong();

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
        hits.compute(ip, (k, queue) -> {
            long now = System.nanoTime();
            ArrayDeque<Long> q = queue != null ? queue : new ArrayDeque<>();
            prune(q, now);
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
        hits.computeIfPresent(ip, (k, q) -> {
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
        hits.computeIfPresent(ip, (k, q) -> {
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

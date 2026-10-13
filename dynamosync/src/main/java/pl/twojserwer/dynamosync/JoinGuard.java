package pl.twojserwer.dynamosync;

import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Chroni przed "join-flood": graczem (albo botem) laczacym sie z serwerem
 * w kolko w krotkich odstepach. Kazdy join odpala asynchroniczne GetItem do
 * DynamoDB (PlayerDataManager.load) - bez tego guardu ktos moglby generowac
 * niepotrzebne zapytania/koszt (RCU) i obciazac pule watkow workera samym
 * spamowaniem reconnectow, zanim jeszcze cokolwiek trafi do PlayerListener.
 *
 * Dziala na AsyncPlayerPreLoginEvent - to jest NAJWCZESNIEJSZY moment w
 * procesie logowania (przed przydzieleniem encji Player, przed jakimkolwiek
 * zapytaniem do DynamoDB), wiec odrzucony gracz nigdy nie dotyka bazy danych.
 *
 * Dwa niezalezne limity:
 *  - per-konto (UUID): blokuje TEGO SAMEGO gracza przed zbyt czestym rejoin,
 *  - per-adres IP: blokuje zbyt czeste proby logowania z tego samego adresu
 *    (niezaleznie od konta) - pomaga przy botach probujacych wielu alt-kont
 *    z jednego IP.
 *
 * Twardnienie (v1.1.0):
 *  - logowania juz odrzucone przez kogos innego (ban, whitelist, pelny serwer) sa pomijane -
 *    wczesniej JoinGuard potrafil nadpisac ich komunikat i "zuzywac" cooldown,
 *  - sprawdzenie + zapis cooldownu sa atomowe (dwa rownolegle polaczenia nie przejda oba),
 *  - nieudane polaczenie z powodu cooldownu IP nie blokuje cooldownu konta,
 *  - IPv6 liczone per /64 (jak w {@link IpRateLimiter}),
 *  - mapy maja twardy limit rozmiaru (fail-open po przepelnieniu).
 */
public class JoinGuard implements Listener {

    /** Twardy limit wpisow w kazdej z map (ochrona pamieci). Po przepelnieniu nowe klucze nie sa sledzone. */
    private static final int MAX_TRACKED = 100_000;

    private final Logger logger;
    private final long accountCooldownMs;
    private final long ipCooldownMs;
    private final boolean debug;

    private final Map<UUID, Long> lastJoinByAccount = new ConcurrentHashMap<>();
    private final Map<String, Long> lastJoinByIp = new ConcurrentHashMap<>();

    public JoinGuard(Logger logger, int accountCooldownSeconds, int ipCooldownSeconds, boolean debug) {
        this.logger = logger;
        this.accountCooldownMs = TimeUnit.SECONDS.toMillis(Math.max(0, accountCooldownSeconds));
        this.ipCooldownMs = TimeUnit.SECONDS.toMillis(Math.max(0, ipCooldownSeconds));
        this.debug = debug;
    }

    @EventHandler(priority = EventPriority.HIGH)
    public void onPreLogin(AsyncPlayerPreLoginEvent event) {
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            return; // ktos inny juz odrzucil - nie nadpisujemy jego komunikatu i nie liczymy cooldownu
        }
        long now = System.currentTimeMillis();
        UUID uuid = event.getUniqueId();
        String ip = event.getAddress() != null ? IpRateLimiter.normalize(event.getAddress().getHostAddress()) : null;

        boolean accountStamped = false;
        if (accountCooldownMs > 0) {
            long remainingMs = checkAndStamp(lastJoinByAccount, uuid, now, accountCooldownMs);
            if (remainingMs > 0) {
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                        "\u00A7cZbyt szybkie ponowne laczenie. Sprobuj za " + toSeconds(remainingMs) + " s.");
                if (debug) {
                    logger.info("[DynamoSync] [JoinGuard] Odrzucono " + event.getName()
                            + " - rejoin cooldown (" + toSeconds(remainingMs) + " s pozostalo).");
                }
                return;
            }
            accountStamped = true;
        }

        if (ipCooldownMs > 0 && ip != null) {
            long remainingMs = checkAndStamp(lastJoinByIp, ip, now, ipCooldownMs);
            if (remainingMs > 0) {
                if (accountStamped) {
                    // To polaczenie i tak odpada - nie karzemy za nie cooldownem konta.
                    lastJoinByAccount.remove(uuid, now);
                }
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                        "\u00A7cZbyt wiele prob polaczenia z tego adresu. Sprobuj za " + toSeconds(remainingMs) + " s.");
                if (debug) {
                    logger.info("[DynamoSync] [JoinGuard] Odrzucono " + event.getName()
                            + " - IP cooldown z adresu " + ip + " (" + toSeconds(remainingMs) + " s pozostalo).");
                }
            }
        }
    }

    /**
     * Atomowo: jesli od ostatniego wpisu minelo mniej niz cooldown - zwraca ile ms zostalo (i nic nie zmienia),
     * w przeciwnym razie zapisuje {@code now} i zwraca 0.
     */
    private <K> long checkAndStamp(Map<K, Long> map, K key, long now, long cooldownMs) {
        if (map.size() >= MAX_TRACKED && !map.containsKey(key)) {
            map.values().removeIf(ts -> now - ts >= cooldownMs); // wygasle wpisy nic juz nie blokuja
            if (map.size() >= MAX_TRACKED) {
                return 0L; // fail-open: lepiej przepuscic niz odciac prawdziwych graczy
            }
        }
        long[] remaining = new long[1];
        map.compute(key, (k, last) -> {
            if (last != null && now - last < cooldownMs) {
                remaining[0] = cooldownMs - (now - last);
                return last;
            }
            return now;
        });
        return remaining[0];
    }

    private static long toSeconds(long ms) {
        return (ms + 999) / 1000;
    }

    /** Okresowe czyszczenie starych wpisow, zeby mapy nie rosly w nieskonczonosc na serwerach z duzym ruchem. */
    public void cleanupOlderThan(long maxAgeMs) {
        long cutoff = System.currentTimeMillis() - maxAgeMs;
        lastJoinByAccount.values().removeIf(ts -> ts < cutoff);
        lastJoinByIp.values().removeIf(ts -> ts < cutoff);
    }
}

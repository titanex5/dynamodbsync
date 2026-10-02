package pl.twojserwer.dynamosync;

import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;

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
 */
public class JoinGuard implements Listener {

    private final Logger logger;
    private final long accountCooldownMs;
    private final long ipCooldownMs;
    private final boolean debug;

    private final java.util.Map<UUID, Long> lastJoinByAccount = new ConcurrentHashMap<>();
    private final java.util.Map<String, Long> lastJoinByIp = new ConcurrentHashMap<>();

    public JoinGuard(Logger logger, int accountCooldownSeconds, int ipCooldownSeconds, boolean debug) {
        this.logger = logger;
        this.accountCooldownMs = TimeUnit.SECONDS.toMillis(Math.max(0, accountCooldownSeconds));
        this.ipCooldownMs = TimeUnit.SECONDS.toMillis(Math.max(0, ipCooldownSeconds));
        this.debug = debug;
    }

    @EventHandler(priority = EventPriority.HIGH)
    public void onPreLogin(AsyncPlayerPreLoginEvent event) {
        long now = System.currentTimeMillis();
        UUID uuid = event.getUniqueId();
        String ip = event.getAddress() != null ? event.getAddress().getHostAddress() : null;

        if (accountCooldownMs > 0) {
            Long lastAccount = lastJoinByAccount.get(uuid);
            if (lastAccount != null && now - lastAccount < accountCooldownMs) {
                long remainingSec = (accountCooldownMs - (now - lastAccount) + 999) / 1000;
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                        "§cZbyt szybkie ponowne laczenie. Sprobuj za " + remainingSec + " s.");
                if (debug) {
                    logger.info("[DynamoSync] [JoinGuard] Odrzucono " + event.getName()
                            + " - rejoin cooldown (" + remainingSec + " s pozostalo).");
                }
                return;
            }
        }

        if (ipCooldownMs > 0 && ip != null) {
            Long lastIp = lastJoinByIp.get(ip);
            if (lastIp != null && now - lastIp < ipCooldownMs) {
                long remainingSec = (ipCooldownMs - (now - lastIp) + 999) / 1000;
                event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                        "§cZbyt wiele prob polaczenia z tego adresu. Sprobuj za " + remainingSec + " s.");
                if (debug) {
                    logger.info("[DynamoSync] [JoinGuard] Odrzucono " + event.getName()
                            + " - IP cooldown z adresu " + ip + " (" + remainingSec + " s pozostalo).");
                }
                return;
            }
        }

        lastJoinByAccount.put(uuid, now);
        if (ip != null) {
            lastJoinByIp.put(ip, now);
        }
    }

    /** Okresowe czyszczenie starych wpisow, zeby mapy nie rosly w nieskonczonosc na serwerach z duzym ruchem. */
    public void cleanupOlderThan(long maxAgeMs) {
        long cutoff = System.currentTimeMillis() - maxAgeMs;
        lastJoinByAccount.values().removeIf(ts -> ts < cutoff);
        lastJoinByIp.values().removeIf(ts -> ts < cutoff);
    }
}

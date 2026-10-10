package pl.twojserwer.dynamosync;

import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.AsyncPlayerPreLoginEvent;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.logging.Logger;

/**
 * Wczesne odrzucenie logowania, gdy adres IP wyczerpal limit zapytan do bazy.
 *
 * Kazde wejscie gracza konczy sie zapytaniem GetItem do DynamoDB, wiec jesli
 * IP jest juz na limicie, nie ma sensu wpuszczac go dalej - odrzucamy w
 * AsyncPlayerPreLoginEvent (przed utworzeniem encji gracza i przed jakimkolwiek
 * zapytaniem). To tylko SPRAWDZENIE (nie zuzywa limitu); wlasciwe zuzycie
 * dzieje sie w PlayerDataManager.load(), w momencie realnego zapytania do bazy.
 */
public class RateLimitListener implements Listener {

    private final IpRateLimiter limiter;
    private final Logger logger;
    private final boolean debug;

    public RateLimitListener(IpRateLimiter limiter, Logger logger, boolean debug) {
        this.limiter = limiter;
        this.logger = logger;
        this.debug = debug;
    }

    // HIGHEST - zeby dzialac po JoinGuard (HIGH) i nie nadpisywac jego decyzji.
    @EventHandler(priority = EventPriority.HIGHEST)
    public void onPreLogin(AsyncPlayerPreLoginEvent event) {
        if (event.getLoginResult() != AsyncPlayerPreLoginEvent.Result.ALLOWED) {
            return; // ktos inny juz odrzucil to logowanie
        }
        String ip = event.getAddress() != null ? event.getAddress().getHostAddress() : null;
        long waitMs = limiter.retryAfterMillis(ip);
        if (waitMs > 0) {
            long waitSec = (waitMs + 999) / 1000;
            event.disallow(AsyncPlayerPreLoginEvent.Result.KICK_OTHER,
                    "§cPrzekroczono limit zapytan do bazy danych z Twojego adresu IP. Sprobuj za " + formatWait(waitSec) + ".");
            if (debug) {
                logger.info("[DynamoSync] [RateLimit] Odrzucono logowanie " + event.getName()
                        + " - IP " + ip + " na limicie (jeszcze " + waitSec + " s).");
            }
        }
    }

    /** Adres IP gracza jako String albo null, gdy nie da sie go ustalic. */
    public static String ipOf(Player player) {
        InetSocketAddress socket = player.getAddress();
        if (socket == null) {
            return null;
        }
        InetAddress address = socket.getAddress();
        return address != null ? address.getHostAddress() : null;
    }

    /** "45 s" albo "3 min 20 s" - czytelniejsze dla gracza niz 200 s. */
    public static String formatWait(long seconds) {
        if (seconds < 60) {
            return seconds + " s";
        }
        long min = seconds / 60;
        long sec = seconds % 60;
        return sec == 0 ? min + " min" : min + " min " + sec + " s";
    }
}

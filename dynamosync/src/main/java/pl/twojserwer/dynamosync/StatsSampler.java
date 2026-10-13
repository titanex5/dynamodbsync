package pl.twojserwer.dynamosync;

import org.bukkit.Bukkit;
import org.bukkit.Statistic;
import org.bukkit.entity.Player;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * Sesje graczy i okresowe "doliczanie" statystyk, ktore nie maja wlasnego eventu: czas gry i przebyty dystans.
 *
 * Wczesniej czas gry byl doliczany WYLACZNIE przy wyjsciu gracza - crash serwera kasowal cala trwajaca sesje.
 * Teraz {@link #sampleAll()} (glowny watek, co {@code stats.sample-interval-seconds}) dolicza czas na biezaco,
 * wiec po crashu tracimy co najwyzej jeden interwal.
 *
 * Wszystkie metody wolac z GLOWNEGO watku (korzystaja z Player#getStatistic).
 */
public final class StatsSampler {

    /** Statystyki vanilla sumowane do dystansu (cm). Delty, wiec nie ma znaczenia, ile gracz mial "z gory". */
    private static final Statistic[] DISTANCE_STATS = {
            Statistic.WALK_ONE_CM, Statistic.SPRINT_ONE_CM, Statistic.SWIM_ONE_CM, Statistic.FLY_ONE_CM,
            Statistic.AVIATE_ONE_CM, Statistic.BOAT_ONE_CM, Statistic.HORSE_ONE_CM, Statistic.MINECART_ONE_CM
    };

    private static final class Session {
        final long joinMillis;
        long accrualMark;      // do tej chwili czas gry jest juz doliczony
        long lastDistanceCm;   // wartosc vanilla przy ostatniej probce

        Session(long now, long distanceCm) {
            this.joinMillis = now;
            this.accrualMark = now;
            this.lastDistanceCm = distanceCm;
        }
    }

    private final PlayerDataManager dataManager;
    private final Logger logger;
    private final boolean trackMovement;
    private final Map<UUID, Session> sessions = new ConcurrentHashMap<>();
    private boolean distanceErrorLogged;

    public StatsSampler(PlayerDataManager dataManager, Logger logger, boolean trackMovement) {
        this.dataManager = dataManager;
        this.logger = logger;
        this.trackMovement = trackMovement;
    }

    /** Poczatek sesji (PlayerJoinEvent). */
    public void begin(Player player) {
        sessions.put(player.getUniqueId(), new Session(System.currentTimeMillis(), readDistanceCm(player)));
    }

    /** Porzuca sesje bez doliczania (np. gracz odrzucony przez limit IP). */
    public void forget(UUID uuid) {
        sessions.remove(uuid);
    }

    /** Koniec sesji (PlayerQuitEvent): dolicza reszte czasu/dystansu, rekord najdluzszej sesji i lastSeen. */
    public void end(Player player) {
        Session session = sessions.remove(player.getUniqueId());
        PlayerData data = dataManager.getCached(player.getUniqueId());
        if (session == null || data == null) {
            return; // gracz wyszedl, zanim asynchroniczny load() sie zakonczyl - nic do doliczenia
        }
        long now = System.currentTimeMillis();
        accrue(player, data, session, now);
        data.recordMax(StatType.LONGEST_SESSION, Math.max(0L, (now - session.joinMillis) / 1000L));
        data.touchLastSeen();
    }

    /** Dolicza czas gry i dystans wszystkim graczom online (zadanie cykliczne + onDisable). */
    public void sampleAll() {
        long now = System.currentTimeMillis();
        for (Player player : Bukkit.getOnlinePlayers()) {
            Session session = sessions.get(player.getUniqueId());
            PlayerData data = dataManager.getCached(player.getUniqueId());
            if (session == null || data == null) {
                continue; // dane jeszcze sie laduja - czas zostanie doliczony przy nastepnej probce
            }
            accrue(player, data, session, now);
        }
    }

    private void accrue(Player player, PlayerData data, Session session, long now) {
        long seconds = (now - session.accrualMark) / 1000L;
        if (seconds > 0) {
            data.addPlaytimeSeconds(seconds);
            session.accrualMark += seconds * 1000L; // reszta (<1 s) zostaje na nastepny raz - nic nie ucieka
        }
        if (trackMovement) {
            long current = readDistanceCm(player);
            long delta = current - session.lastDistanceCm;
            if (delta > 0) {
                data.addStat(StatType.DISTANCE_CM, delta);
            }
            session.lastDistanceCm = current;
        }
    }

    private long readDistanceCm(Player player) {
        if (!trackMovement) {
            return 0L;
        }
        long total = 0L;
        try {
            for (Statistic stat : DISTANCE_STATS) {
                total += player.getStatistic(stat);
            }
        } catch (RuntimeException ex) {
            if (!distanceErrorLogged) {
                distanceErrorLogged = true;
                logger.warning("[DynamoSync] Nie mozna odczytac statystyk dystansu (" + ex.getClass().getSimpleName()
                        + ": " + ex.getMessage() + ") - dystans nie bedzie liczony.");
            }
        }
        return total;
    }
}

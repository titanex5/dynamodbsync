package pl.twojserwer.dynamosync;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

public class PlayerListener implements Listener {

    private final JavaPlugin plugin;
    private final PlayerDataManager dataManager;
    private final Map<UUID, Long> sessionStart = new ConcurrentHashMap<>();

    public PlayerListener(JavaPlugin plugin, PlayerDataManager dataManager) {
        this.plugin = plugin;
        this.dataManager = dataManager;
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onJoin(PlayerJoinEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();
        sessionStart.put(uuid, System.currentTimeMillis());

        // IP gracza - do limitu zapytan per adres IP (zapytanie GetItem zalicza sie do limitu tego IP).
        String ip = RateLimitListener.ipOf(player);

        dataManager.load(uuid, player.getName(), ip).thenAccept(data -> {
            data.setSessionIp(ip);
            data.setUsername(player.getName());
            data.touchLastSeen();
        }).exceptionally(ex -> {
            Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
            if (cause instanceof RateLimitExceededException limit) {
                // Normalnie IP odpada juz w RateLimitListener (PreLogin). Tu lapiemy tylko wyscig:
                // kilka rownoczesnych logowan z jednego IP, ktore przeszly sprawdzenie w tym samym momencie.
                Bukkit.getScheduler().runTask(plugin, () -> {
                    sessionStart.remove(uuid);
                    player.kick(Component.text("Przekroczono limit zapytan do bazy danych z Twojego adresu IP. Sprobuj za "
                            + RateLimitListener.formatWait(limit.getRetryAfterSeconds()) + ".", NamedTextColor.RED));
                });
            } else {
                plugin.getLogger().warning("[DynamoSync] Nieoczekiwany blad przy wejsciu gracza " + player.getName()
                        + ": " + cause);
            }
            return null;
        });
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onQuit(PlayerQuitEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();

        Long start = sessionStart.remove(uuid);
        PlayerData data = dataManager.getCached(uuid);
        if (data == null) {
            // Gracz wyszedl zanim asynchroniczny load() zdazyl sie zakonczyc - nic do zapisania.
            return;
        }
        if (start != null) {
            long elapsedSeconds = (System.currentTimeMillis() - start) / 1000L;
            data.addPlaytimeSeconds(elapsedSeconds);
        }

        // saveIfDirty - jesli gracz nic nie zmienil w tej sesji (np. wszedl i od razu
        // wyszedl bez zadnej akcji), nie placimy za niepotrzebny WCU.
        dataManager.saveIfDirty(data).thenRun(() -> dataManager.evictFromCache(uuid));
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onDeath(PlayerDeathEvent event) {
        Player victim = event.getEntity();
        PlayerData victimData = dataManager.getCached(victim.getUniqueId());
        if (victimData != null) {
            victimData.addDeath();
        }

        Player killer = victim.getKiller();
        if (killer != null && !killer.getUniqueId().equals(victim.getUniqueId())) {
            PlayerData killerData = dataManager.getCached(killer.getUniqueId());
            if (killerData != null) {
                killerData.addKill();
            }
        }
        // Nie zapisujemy od razu do DynamoDB przy kazdej smierci - zbierne sie to
        // przy nastepnym autosave albo wyjsciu gracza, zeby nie zapychac API w intensywnym PvP.
    }
}

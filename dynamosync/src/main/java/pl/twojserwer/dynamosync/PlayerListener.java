package pl.twojserwer.dynamosync;

import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.Map;
import java.util.UUID;
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

        dataManager.load(uuid, player.getName()).thenAccept(data -> {
            data.setUsername(player.getName());
            data.setLastJoin(System.currentTimeMillis());
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

        dataManager.save(data).thenRun(() -> dataManager.evictFromCache(uuid));
    }
}

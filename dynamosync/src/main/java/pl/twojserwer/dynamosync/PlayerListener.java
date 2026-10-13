package pl.twojserwer.dynamosync;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.EntityDamageEvent;
import org.bukkit.event.entity.PlayerDeathEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.UUID;
import java.util.concurrent.CompletionException;

public class PlayerListener implements Listener {

    private final JavaPlugin plugin;
    private final PlayerDataManager dataManager;
    private final StatsSampler sampler;

    public PlayerListener(JavaPlugin plugin, PlayerDataManager dataManager, StatsSampler sampler) {
        this.plugin = plugin;
        this.dataManager = dataManager;
        this.sampler = sampler;
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onJoin(PlayerJoinEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();
        sampler.begin(player);

        // IP gracza - do limitu zapytan per adres IP (zapytanie GetItem zalicza sie do limitu tego IP).
        String ip = RateLimitListener.ipOf(player);

        // Szybki rejoin: wpis wychodzacego gracza moze jeszcze siedziec w cache. Oznaczamy go jako online
        // OD RAZU (glowny watek), zanim autosave zdazy go usunac jako "offline i zapisany".
        PlayerData alreadyCached = dataManager.getCached(uuid);
        if (alreadyCached != null) {
            alreadyCached.markOnline();
        }

        dataManager.load(uuid, player.getName(), ip).thenAccept(data -> {
            data.markOnline();
            data.setSessionIp(ip);
            data.setUsername(player.getName());
            data.touchLastSeen();
            data.addStat(StatType.JOINS, 1);

            // Gracz mogl wyjsc, zanim odczyt sie skonczyl (onQuit nie mial wtedy czego zapisac) - bez tego
            // wpis zostalby w cache na zawsze. Sprawdzamy na glownym watku, czy dalej go nie ma online.
            Bukkit.getScheduler().runTask(plugin, () -> {
                if (Bukkit.getPlayer(uuid) == null) {
                    sampler.forget(uuid);
                    data.markOffline();
                    dataManager.saveIfDirty(data).thenRun(() -> dataManager.evictIfSettled(data));
                }
            });
        }).exceptionally(ex -> {
            Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
            if (cause instanceof RateLimitExceededException limit) {
                // Normalnie IP odpada juz w RateLimitListener (PreLogin). Tu lapiemy tylko wyscig:
                // kilka rownoczesnych logowan z jednego IP, ktore przeszly sprawdzenie w tym samym momencie.
                Bukkit.getScheduler().runTask(plugin, () -> {
                    sampler.forget(uuid);
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

        // Dolicza reszte czasu gry/dystansu, rekord najdluzszej sesji i lastSeen (wczesniej lastSeen to byl
        // moment WEJSCIA, nie wyjscia).
        sampler.end(player);
        PlayerData data = dataManager.getCached(uuid);
        if (data == null) {
            // Gracz wyszedl zanim asynchroniczny load() zdazyl sie zakonczyc - nic do zapisania.
            return;
        }

        // saveIfDirty - jesli gracz nic nie zmienil w tej sesji (np. wszedl i od razu
        // wyszedl bez zadnej akcji), nie placimy za niepotrzebny WCU.
        // Wpis usuwamy z cache DOPIERO gdy jest naprawde zapisany (evictIfSettled). Gdy zapis sie nie uda
        // (awaria/throttling bazy), dane zostaja w pamieci i dociaga je autosave - wczesniej znikaly razem z wyjsciem gracza.
        data.markOffline();
        dataManager.saveIfDirty(data).thenRun(() -> dataManager.evictIfSettled(data));
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onDeath(PlayerDeathEvent event) {
        Player victim = event.getEntity();
        PlayerData victimData = dataManager.getCached(victim.getUniqueId());
        if (victimData != null) {
            // Przyczyna smierci (np. FALL, LAVA, ENTITY_ATTACK) - do statystyki "najczestsza przyczyna".
            EntityDamageEvent lastDamage = victim.getLastDamageCause();
            victimData.registerDeath(lastDamage != null ? lastDamage.getCause().name() : null);
        }

        Player killer = victim.getKiller();
        if (killer != null && !killer.getUniqueId().equals(victim.getUniqueId())) {
            PlayerData killerData = dataManager.getCached(killer.getUniqueId());
            if (killerData != null) {
                killerData.registerKill();
            }
        }
        // Nie zapisujemy od razu do DynamoDB przy kazdej smierci - zbierne sie to
        // przy nastepnym autosave albo wyjsciu gracza, zeby nie zapychac API w intensywnym PvP.
    }
}

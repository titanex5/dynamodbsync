package pl.twojserwer.dynamosync.commands;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import pl.twojserwer.dynamosync.InputValidator;
import pl.twojserwer.dynamosync.PlayerData;
import pl.twojserwer.dynamosync.PlayerDataManager;
import pl.twojserwer.dynamosync.RateLimitExceededException;
import pl.twojserwer.dynamosync.RateLimitListener;
import pl.twojserwer.dynamosync.StatsFormatter;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Komenda /stats [gracz] dla zwyklych graczy (uprawnienie dynamosync.stats, domyslnie dla wszystkich).
 *
 *  - /stats            - wlasne statystyki z cache (0 zapytan do bazy),
 *  - /stats <gracz>    - gracz online: z cache (0 zapytan); offline: jeden odczyt z DynamoDB, ktory zalicza sie
 *                        do limitu IP pytajacego i nie zasmieca cache ({@code loadReadOnly}). Wymaga
 *                        uprawnienia dynamosync.stats.others.
 */
public class StatsCommand implements CommandExecutor, TabCompleter {

    private static final long COOLDOWN_MS = TimeUnit.SECONDS.toMillis(3);

    private final JavaPlugin plugin;
    private final PlayerDataManager dataManager;
    private final java.util.Map<UUID, Long> lastUse = new ConcurrentHashMap<>();

    public StatsCommand(JavaPlugin plugin, PlayerDataManager dataManager) {
        this.plugin = plugin;
        this.dataManager = dataManager;
    }

    private boolean isOnCooldown(CommandSender sender) {
        if (!(sender instanceof Player player)) {
            return false;
        }
        long now = System.currentTimeMillis();
        if (lastUse.size() > 256) {
            lastUse.values().removeIf(ts -> now - ts > 60_000L);
        }
        Long last = lastUse.get(player.getUniqueId());
        if (last != null && now - last < COOLDOWN_MS) {
            sender.sendMessage("§cZbyt szybko - odczekaj jeszcze " + ((COOLDOWN_MS - (now - last)) / 1000.0) + " s.");
            return true;
        }
        lastUse.put(player.getUniqueId(), now);
        return false;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!sender.hasPermission("dynamosync.stats")) {
            sender.sendMessage("§cBrak uprawnien.");
            return true;
        }
        if (args.length > 1) {
            sender.sendMessage("§eUzycie: /stats [gracz]");
            return true;
        }

        OfflinePlayer target;
        String targetName;
        if (args.length == 0) {
            if (!(sender instanceof Player self)) {
                sender.sendMessage("§eKonsola musi podac gracza: /stats <gracz>");
                return true;
            }
            target = self;
            targetName = self.getName();
        } else {
            if (!sender.hasPermission("dynamosync.stats.others")
                    && !(sender instanceof Player p && p.getName().equalsIgnoreCase(args[0]))) {
                sender.sendMessage("§cNie masz uprawnien do ogladania cudzych statystyk.");
                return true;
            }
            targetName = args[0];
            if (!InputValidator.isValidPlayerName(targetName)) {
                sender.sendMessage("§cNieprawidlowy nick gracza.");
                return true;
            }
            // Tylko gracze znani serwerowi - bez blokujacego zapytania do Mojang na glownym watku.
            Player online = Bukkit.getPlayerExact(targetName);
            target = online != null ? online : Bukkit.getOfflinePlayerIfCached(targetName);
            if (target == null) {
                sender.sendMessage("§cGracz '" + InputValidator.escapeForChat(targetName)
                        + "' jest nieznany temu serwerowi (nigdy nie wchodzil).");
                return true;
            }
        }

        UUID uuid = target.getUniqueId();
        PlayerData cached = dataManager.getCached(uuid);
        if (cached != null) {
            // Trafienie w cache = 0 zapytan do bazy, wiec bez cooldownu (cooldown dotyczy tylko odczytow z DB nizej).
            StatsFormatter.lines(targetName, cached).forEach(sender::sendMessage);
            return true;
        }
        if (isOnCooldown(sender)) {
            return true;
        }
        String ip = sender instanceof Player asker ? RateLimitListener.ipOf(asker) : null;
        dataManager.loadReadOnly(uuid, targetName, ip)
                .thenAccept(data -> Bukkit.getScheduler().runTask(plugin,
                        () -> StatsFormatter.lines(targetName, data).forEach(sender::sendMessage)))
                .exceptionally(ex -> {
                    Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
                    if (cause instanceof RateLimitExceededException limit) {
                        Bukkit.getScheduler().runTask(plugin, () -> sender.sendMessage(
                                "§cPrzekroczono limit zapytan do bazy danych z Twojego adresu IP. Sprobuj za "
                                        + RateLimitListener.formatWait(limit.getRetryAfterSeconds()) + "."));
                    } else {
                        plugin.getLogger().warning("[DynamoSync] Blad komendy /stats: " + cause);
                        Bukkit.getScheduler().runTask(plugin, () -> sender.sendMessage(
                                "§cBlad bazy danych - sprobuj ponownie pozniej."));
                    }
                    return null;
                });
        return true;
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        if (args.length == 1 && sender.hasPermission("dynamosync.stats.others")) {
            return Bukkit.getOnlinePlayers().stream().map(Player::getName).toList();
        }
        return List.of();
    }
}

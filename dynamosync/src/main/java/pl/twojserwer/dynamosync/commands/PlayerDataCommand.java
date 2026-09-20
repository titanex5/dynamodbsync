package pl.twojserwer.dynamosync.commands;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;
import pl.twojserwer.dynamosync.PlayerData;
import pl.twojserwer.dynamosync.PlayerDataManager;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Komenda /playerdata. Oprocz oryginalnej logiki dodano:
 *  - cooldown per-nadawca (anty-flood) - blokuje zasypywanie DynamoDB zapytaniami,
 *  - walidacje klucza/wartosci w "set" przez PlayerDataManager.validateCustomField()
 *    zamiast wysylania surowego wejscia gracza prosto do bazy.
 */
public class PlayerDataCommand implements CommandExecutor, TabCompleter {

    /** Minimalny odstep miedzy komendami dla tego samego nadawcy (anty-flood / ochrona przed nadmiernym RCU/WCU). */
    private static final long COOLDOWN_MS = TimeUnit.SECONDS.toMillis(2);

    private final JavaPlugin plugin;
    private final PlayerDataManager dataManager;
    private final java.util.Map<UUID, Long> lastUse = new ConcurrentHashMap<>();

    public PlayerDataCommand(JavaPlugin plugin, PlayerDataManager dataManager) {
        this.plugin = plugin;
        this.dataManager = dataManager;
    }

    /** Zwraca true jesli nadawca jest jeszcze na cooldownie (i wysyla mu o tym wiadomosc). Konsole nie sa limitowane. */
    private boolean isOnCooldown(CommandSender sender) {
        if (!(sender instanceof Player player)) {
            return false; // konsola/serwer - brak limitu
        }
        long now = System.currentTimeMillis();
        Long last = lastUse.get(player.getUniqueId());
        if (last != null && now - last < COOLDOWN_MS) {
            long remainingMs = COOLDOWN_MS - (now - last);
            sender.sendMessage("§cZbyt szybko - odczekaj jeszcze " + (remainingMs / 1000.0) + " s.");
            return true;
        }
        lastUse.put(player.getUniqueId(), now);
        return false;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!sender.hasPermission("dynamosync.admin")) {
            sender.sendMessage("§cBrak uprawnien.");
            return true;
        }
        if (isOnCooldown(sender)) {
            return true;
        }
        if (args.length >= 1 && args[0].equalsIgnoreCase("debug")) {
            PlayerDataManager.DebugStats s = dataManager.getDebugStats();
            sender.sendMessage("§7=== DynamoSync debug ===");
            sender.sendMessage("§eCache (graczy w pamieci): §f" + s.cacheSize());
            sender.sendMessage("§eOdczyty z DynamoDB (GetItem): §f" + s.totalLoadsFromDb()
                    + " §7| z cache (0 RCU): §f" + s.totalLoadsFromCache());
            sender.sendMessage("§eZapisy wyslane do DynamoDB: §f" + s.totalWritesSent());
            sender.sendMessage("§eZapisy pominiete (brak zmian, oszczedzone WCU): §f" + s.totalWritesSkippedClean());
            sender.sendMessage("§eZapisy odrzucone (walidacja - za duze/niepoprawne): §f" + s.totalWritesRejectedInvalid());
            sender.sendMessage("§eBledy: §f" + s.totalErrors());
            sender.sendMessage("§eAutosave: §f" + s.totalAutosaveRuns() + " uruchomien, ostatni: "
                    + (s.lastAutosaveDurationMs() < 0 ? "brak jeszcze" :
                       s.lastAutosaveDirtyCount() + " zapisanych w " + s.lastAutosaveDurationMs() + " ms"));
            return true;
        }

        if (args.length < 3) {
            if (args.length == 2 && args[0].equalsIgnoreCase("stats")) {
                String targetName = args[1];
                OfflinePlayer target = Bukkit.getOfflinePlayer(targetName);
                dataManager.load(target.getUniqueId(), targetName).thenAccept(data -> Bukkit.getScheduler().runTask(plugin, () -> {
                    sender.sendMessage("§7=== Profil: " + targetName + " §7===");
                    sender.sendMessage("§eUtworzono: §f" + data.getCreatedDateFormatted());
                    sender.sendMessage("§eOstatnio widziany: §f" + data.getLastSeenFormatted());
                    sender.sendMessage("§eCzas gry: §f" + (data.getPlaytime() / 3600) + " h");
                    sender.sendMessage("§eZabojstwa: §f" + data.getKills());
                    sender.sendMessage("§eSmierci: §f" + data.getDeaths());
                    sender.sendMessage(String.format("§eK/D: §f%.2f", data.getKdRatio()));
                }));
                return true;
            }
            sender.sendMessage("§eUzycie: /playerdata <get|set|stats> <gracz> [klucz] [wartosc]  |  /playerdata debug");
            return true;
        }

        String action = args[0].toLowerCase();
        String targetName = args[1];
        String key = args[2];

        OfflinePlayer target = Bukkit.getOfflinePlayer(targetName);
        UUID uuid = target.getUniqueId();

        if (action.equals("get")) {
            dataManager.load(uuid, targetName).thenAccept(data -> Bukkit.getScheduler().runTask(plugin, () -> {
                String value = data.getCustom(key);
                sender.sendMessage("§7[" + targetName + "] " + key + " = §f" + (value == null ? "§8(brak)" : value));
            }));
            return true;
        }

        if (action.equals("set")) {
            if (args.length < 4) {
                sender.sendMessage("§eUzycie: /playerdata set <gracz> <klucz> <wartosc>");
                return true;
            }
            String value = String.join(" ", Arrays.copyOfRange(args, 3, args.length));

            // Walidacja PRZED dotknieciem cache/DynamoDB - odrzucamy zle dane od razu,
            // zamiast liczyc na to, ze AWS sam je odrzuci.
            String validationError = dataManager.validateCustomField(key, value);
            if (validationError != null) {
                sender.sendMessage("§cOdrzucono: " + validationError);
                return true;
            }

            dataManager.load(uuid, targetName).thenAccept(data -> {
                data.setCustom(key, value);
                dataManager.save(data).thenRun(() -> Bukkit.getScheduler().runTask(plugin, () ->
                        sender.sendMessage("§aUstawiono " + key + " = " + value + " dla " + targetName)));
            });
            return true;
        }

        sender.sendMessage("§eUzycie: /playerdata <get|set> <gracz> <klucz> [wartosc]");
        return true;
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        if (args.length == 1) {
            return Arrays.asList("get", "set", "stats", "debug");
        }
        if (args.length == 2) {
            return Bukkit.getOnlinePlayers().stream().map(p -> p.getName()).toList();
        }
        return List.of();
    }
}

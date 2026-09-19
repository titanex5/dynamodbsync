package pl.twojserwer.dynamosync.commands;

import org.bukkit.Bukkit;
import org.bukkit.OfflinePlayer;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.plugin.java.JavaPlugin;
import pl.twojserwer.dynamosync.PlayerData;
import pl.twojserwer.dynamosync.PlayerDataManager;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class PlayerDataCommand implements CommandExecutor, TabCompleter {

    private final JavaPlugin plugin;
    private final PlayerDataManager dataManager;

    public PlayerDataCommand(JavaPlugin plugin, PlayerDataManager dataManager) {
        this.plugin = plugin;
        this.dataManager = dataManager;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!sender.hasPermission("dynamosync.admin")) {
            sender.sendMessage("§cBrak uprawnien.");
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
                    sender.sendMessage("§7=== Statystyki PvP: " + targetName + " §7===");
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

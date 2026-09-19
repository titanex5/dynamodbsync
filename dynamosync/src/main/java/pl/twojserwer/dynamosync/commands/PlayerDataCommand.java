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
        if (args.length < 3) {
            sender.sendMessage("§eUzycie: /playerdata <get|set> <gracz> <klucz> [wartosc]");
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
            return Arrays.asList("get", "set");
        }
        if (args.length == 2) {
            return Bukkit.getOnlinePlayers().stream().map(p -> p.getName()).toList();
        }
        return List.of();
    }
}

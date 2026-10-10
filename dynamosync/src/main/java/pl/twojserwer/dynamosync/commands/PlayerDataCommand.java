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
import pl.twojserwer.dynamosync.IpRateLimiter;
import pl.twojserwer.dynamosync.PlayerData;
import pl.twojserwer.dynamosync.PlayerDataManager;
import pl.twojserwer.dynamosync.RateLimitExceededException;
import pl.twojserwer.dynamosync.RateLimitListener;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Komenda /playerdata. Oprocz oryginalnej logiki dodano:
 *  - cooldown per-nadawca (anty-flood) - blokuje zasypywanie DynamoDB zapytaniami,
 *  - walidacje klucza/wartosci w "set" przez InputValidator zamiast wysylania surowego wejscia prosto do bazy.
 *
 * Twardnienie (v1.1.0):
 *  - nick celu jest walidowany wzorcem prawdziwego nicku, zanim trafi do Bukkit API,
 *  - cel jest szukany WYLACZNIE wsrod graczy znanych serwerowi (online / usercache). Wczesniej
 *    Bukkit.getOfflinePlayer(String) dla nieznanego nicku robilo blokujace zapytanie do Mojang na GLOWNYM
 *    watku - literowka (albo zlosliwy admin) mogla zamrozic serwer,
 *  - get/stats nie tworza pustych profili i nie zasmiecaja cache (zob. PlayerDataManager#loadReadOnly),
 *  - kazda zmiana przez "set" trafia do logu konsoli (audyt: kto, komu, co) - z odfiltrowanymi znakami
 *    kontrolnymi (ochrona przed log-injection),
 *  - wartosci z bazy sa wyswietlane bez kodow kolorow '§'.
 */
public class PlayerDataCommand implements CommandExecutor, TabCompleter {

    /** Minimalny odstep miedzy komendami dla tego samego nadawcy (anty-flood / ochrona przed nadmiernym RCU/WCU). */
    private static final long COOLDOWN_MS = TimeUnit.SECONDS.toMillis(2);

    private static final String USAGE =
            "§eUzycie: /playerdata <get|set|stats> <gracz> [klucz] [wartosc]  |  /playerdata debug";

    private final JavaPlugin plugin;
    private final PlayerDataManager dataManager;
    private final boolean auditLog;
    private final java.util.Map<UUID, Long> lastUse = new ConcurrentHashMap<>();

    public PlayerDataCommand(JavaPlugin plugin, PlayerDataManager dataManager) {
        this.plugin = plugin;
        this.dataManager = dataManager;
        this.auditLog = plugin.getConfig().getBoolean("settings.audit-log", true);
    }

    /** Zwraca true jesli nadawca jest jeszcze na cooldownie (i wysyla mu o tym wiadomosc). Konsole nie sa limitowane. */
    private boolean isOnCooldown(CommandSender sender) {
        if (!(sender instanceof Player player)) {
            return false; // konsola/serwer - brak limitu
        }
        long now = System.currentTimeMillis();
        if (lastUse.size() > 256) {
            lastUse.values().removeIf(ts -> now - ts > 60_000L); // mapa nie rosnie w nieskonczonosc
        }
        Long last = lastUse.get(player.getUniqueId());
        if (last != null && now - last < COOLDOWN_MS) {
            long remainingMs = COOLDOWN_MS - (now - last);
            sender.sendMessage("§cZbyt szybko - odczekaj jeszcze " + (remainingMs / 1000.0) + " s.");
            return true;
        }
        lastUse.put(player.getUniqueId(), now);
        return false;
    }

    /** IP, ktoremu zaliczamy zapytania z tej komendy. Konsola nie ma IP, wiec nie jest limitowana. */
    private static String ipOf(CommandSender sender) {
        return sender instanceof Player player ? RateLimitListener.ipOf(player) : null;
    }

    /** Gracz znany serwerowi (online albo w usercache) - BEZ zapytan sieciowych. null jesli nieznany. */
    private static OfflinePlayer resolveKnownPlayer(String name) {
        Player online = Bukkit.getPlayerExact(name);
        if (online != null) {
            return online;
        }
        return Bukkit.getOfflinePlayerIfCached(name);
    }

    /** Obsluga bledu z load(): przy przekroczeniu limitu IP informuje nadawce; inne bledy loguje i tez informuje. */
    private Void handleLoadFailure(CommandSender sender, Throwable ex) {
        Throwable cause = (ex instanceof CompletionException && ex.getCause() != null) ? ex.getCause() : ex;
        if (cause instanceof RateLimitExceededException limit) {
            Bukkit.getScheduler().runTask(plugin, () -> sender.sendMessage(
                    "§cPrzekroczono limit zapytan do bazy danych z Twojego adresu IP. Sprobuj za "
                            + RateLimitListener.formatWait(limit.getRetryAfterSeconds()) + "."));
        } else {
            plugin.getLogger().warning("[DynamoSync] Blad komendy /playerdata: " + cause);
            Bukkit.getScheduler().runTask(plugin, () -> sender.sendMessage(
                    "§cBlad bazy danych - szczegoly w konsoli serwera."));
        }
        return null;
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
            sendDebug(sender);
            return true;
        }

        if (args.length < 2) {
            sender.sendMessage(USAGE);
            return true;
        }

        String action = args[0].toLowerCase(Locale.ROOT);
        if (!action.equals("get") && !action.equals("set") && !action.equals("stats")) {
            sender.sendMessage(USAGE);
            return true;
        }

        String targetName = args[1];
        if (!InputValidator.isValidPlayerName(targetName)) {
            sender.sendMessage("§cNieprawidlowy nick gracza.");
            return true;
        }
        OfflinePlayer target = resolveKnownPlayer(targetName);
        if (target == null) {
            sender.sendMessage("§cGracz '" + targetName + "' jest nieznany temu serwerowi (nigdy nie wchodzil).");
            return true;
        }
        UUID uuid = target.getUniqueId();
        String senderIp = ipOf(sender);

        switch (action) {
            case "stats" -> {
                if (args.length != 2) {
                    sender.sendMessage("§eUzycie: /playerdata stats <gracz>");
                    return true;
                }
                dataManager.loadReadOnly(uuid, targetName, senderIp).thenAccept(data -> Bukkit.getScheduler().runTask(plugin, () -> {
                    sender.sendMessage("§7=== Profil: " + targetName + " §7===");
                    sender.sendMessage("§eUtworzono: §f" + data.getCreatedDateFormatted());
                    sender.sendMessage("§eOstatnio widziany: §f" + data.getLastSeenFormatted());
                    sender.sendMessage("§eCzas gry: §f" + (data.getPlaytime() / 3600) + " h");
                    sender.sendMessage("§eZabojstwa: §f" + data.getKills());
                    sender.sendMessage("§eSmierci: §f" + data.getDeaths());
                    sender.sendMessage(String.format("§eK/D: §f%.2f", data.getKdRatio()));
                })).exceptionally(ex -> handleLoadFailure(sender, ex));
                return true;
            }
            case "get" -> {
                if (args.length != 3) {
                    sender.sendMessage("§eUzycie: /playerdata get <gracz> <klucz>");
                    return true;
                }
                String key = args[2];
                String keyError = InputValidator.validateCustomKey(key);
                if (keyError != null) {
                    sender.sendMessage("§cOdrzucono: " + keyError);
                    return true;
                }
                dataManager.loadReadOnly(uuid, targetName, senderIp).thenAccept(data -> Bukkit.getScheduler().runTask(plugin, () -> {
                    String value = data.getCustom(key);
                    sender.sendMessage("§7[" + targetName + "] " + key + " = §f"
                            + (value == null ? "§8(brak)" : InputValidator.escapeForChat(value)));
                })).exceptionally(ex -> handleLoadFailure(sender, ex));
                return true;
            }
            default -> { // "set"
                if (args.length < 4) {
                    sender.sendMessage("§eUzycie: /playerdata set <gracz> <klucz> <wartosc>");
                    return true;
                }
                String key = args[2];
                String value = String.join(" ", Arrays.copyOfRange(args, 3, args.length));

                // Walidacja PRZED dotknieciem cache/DynamoDB - odrzucamy zle dane od razu,
                // zamiast liczyc na to, ze AWS sam je odrzuci.
                String validationError = dataManager.validateCustomField(key, value);
                if (validationError != null) {
                    sender.sendMessage("§cOdrzucono: " + validationError);
                    return true;
                }

                dataManager.load(uuid, targetName, senderIp).thenAccept(data -> {
                    try {
                        data.setCustom(key, value);
                    } catch (IllegalArgumentException ex) { // np. przekroczony limit liczby pol
                        Bukkit.getScheduler().runTask(plugin, () -> sender.sendMessage("§cOdrzucono: " + ex.getMessage()));
                        return;
                    }
                    if (auditLog) {
                        plugin.getLogger().info("[DynamoSync] [AUDIT] " + InputValidator.sanitizeForLog(sender.getName(), 32)
                                + " ustawil '" + key + "' = '" + InputValidator.sanitizeForLog(value, 100)
                                + "' dla " + targetName + " (" + uuid + ")");
                    }
                    // Zapis zalicza sie do limitu IP admina (i nigdy nie jest przez limit odrzucany).
                    dataManager.save(data, senderIp).thenRun(() -> Bukkit.getScheduler().runTask(plugin, () -> {
                        sender.sendMessage("§aUstawiono " + key + " = " + InputValidator.escapeForChat(value)
                                + " dla " + targetName);
                        // Profil offline zaladowany tylko na potrzeby komendy nie powinien siedziec w cache.
                        if (Bukkit.getPlayer(uuid) == null) {
                            data.markOffline();
                            dataManager.evictIfSettled(data);
                        }
                    }));
                }).exceptionally(ex -> handleLoadFailure(sender, ex));
                return true;
            }
        }
    }

    private void sendDebug(CommandSender sender) {
        PlayerDataManager.DebugStats s = dataManager.getDebugStats();
        sender.sendMessage("§7=== DynamoSync debug ===");
        sender.sendMessage("§eCache (graczy w pamieci): §f" + s.cacheSize());
        sender.sendMessage("§eOdczyty z DynamoDB (GetItem): §f" + s.totalLoadsFromDb()
                + " §7| z cache (0 RCU): §f" + s.totalLoadsFromCache());
        sender.sendMessage("§eZapisy wyslane do DynamoDB: §f" + s.totalWritesSent());
        sender.sendMessage("§eZapisy pominiete (brak zmian, oszczedzone WCU): §f" + s.totalWritesSkippedClean());
        sender.sendMessage("§eZapisy odrzucone (walidacja - za duze/niepoprawne): §f" + s.totalWritesRejectedInvalid());
        sender.sendMessage("§eBledy: §f" + s.totalErrors()
                + " §7| odczyty w trybie awaryjnym: §f" + s.totalDegradedLoads()
                + " §7| odzyskane: §f" + s.totalRecovered());
        sender.sendMessage("§eAutosave: §f" + s.totalAutosaveRuns() + " uruchomien, ostatni: "
                + (s.lastAutosaveDurationMs() < 0 ? "brak jeszcze" :
                   s.lastAutosaveDirtyCount() + " zapisanych w " + s.lastAutosaveDurationMs() + " ms"));
        IpRateLimiter limiter = dataManager.getRateLimiter();
        if (limiter == null) {
            sender.sendMessage("§eLimit zapytan per IP: §fwylaczony");
        } else {
            sender.sendMessage("§eLimit zapytan per IP: §f" + limiter.getMaxRequests() + " / " + limiter.getWindowSeconds()
                    + " s §7| sledzonych IP: §f" + limiter.trackedIpCount()
                    + " §7| odrzuconych zapytan: §f" + limiter.getTotalDenied()
                    + " §7| pominietych (tablica pelna): §f" + limiter.getOverflowSkips());
        }
    }

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        if (!sender.hasPermission("dynamosync.admin")) {
            return List.of(); // nie zdradzamy listy graczy/podkomend osobom bez uprawnien
        }
        if (args.length == 1) {
            return Arrays.asList("get", "set", "stats", "debug");
        }
        if (args.length == 2) {
            return Bukkit.getOnlinePlayers().stream().map(p -> p.getName()).toList();
        }
        return List.of();
    }
}

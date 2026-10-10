package pl.twojserwer.dynamosync;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import pl.twojserwer.dynamosync.commands.PlayerDataCommand;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public final class DynamoSyncPlugin extends JavaPlugin {

    private static DynamoSyncPlugin instance;

    // Reguly DynamoDB dla nazwy tabeli: 3-255 znakow [a-zA-Z0-9_.-]. Wszystko inne to blad konfiguracji.
    private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("^[A-Za-z0-9_.\\-]{3,255}$");

    private DynamoDbClient dynamoDbClient;
    private ExecutorService executor;
    private PlayerDataManager dataManager;
    private JoinGuard joinGuard;
    private IpRateLimiter ipRateLimiter;

    public static DynamoSyncPlugin getInstance() {
        return instance;
    }

    @Override
    public void onEnable() {
        instance = this;
        saveDefaultConfig();

        String region = getConfig().getString("aws.region", "eu-central-1");
        String tableName = getConfig().getString("aws.table-name", "player_data");
        String accessKey = getConfig().getString("aws.access-key", "");
        String secretKey = getConfig().getString("aws.secret-key", "");
        boolean createTable = getConfig().getBoolean("settings.create-table-if-missing", true);
        boolean debug = getConfig().getBoolean("settings.debug", false);
        int autosaveSeconds = getConfig().getInt("settings.autosave-interval-seconds", 300);
        // Twarde widelki: zbyt krotki timeout psulby zdrowe zapytania, zbyt dlugi pozwalalby zawieszonej bazie
        // zablokowac cala pule watkow workera.
        int dbTimeoutSeconds = Math.min(60, Math.max(2, getConfig().getInt("settings.db-timeout-seconds", 10)));
        // Autosave czesciej niz co 10 s to czysta strata WCU (i latwy sposob na wlasnoreczny throttling).
        if (autosaveSeconds > 0 && autosaveSeconds < 10) {
            getLogger().warning("[DynamoSync] autosave-interval-seconds=" + autosaveSeconds
                    + " jest za male - uzywam 10 s.");
            autosaveSeconds = 10;
        }

        if (!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
            getLogger().severe("[DynamoSync] Nieprawidlowa nazwa tabeli '" + tableName
                    + "' (dozwolone: 3-255 znakow: litery, cyfry, '_', '-', '.'). Wylaczam plugin.");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        if (!accessKey.isBlank() || !secretKey.isBlank()) {
            getLogger().warning("[DynamoSync] Klucze AWS sa wpisane w config.yml. Bezpieczniej uzyc zmiennych "
                    + "srodowiskowych AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY (zob. README) - plik z kluczami "
                    + "latwo wyciekl przez backup, repo albo wtyczke z dostepem do plikow.");
        }

        // Watki na wywolania sieciowe do AWS - nigdy nie blokujemy glownego watku serwera.
        executor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "DynamoSync-Worker");
            t.setDaemon(true);
            return t;
        });

        try {
            DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder()
                    .region(Region.of(region))
                    .httpClientBuilder(UrlConnectionHttpClient.builder()
                            .connectionTimeout(Duration.ofSeconds(3))
                            .socketTimeout(Duration.ofSeconds(dbTimeoutSeconds)))
                    // Zawieszona/wolna baza nie moze trzymac watkow workera w nieskonczonosc.
                    .overrideConfiguration(ClientOverrideConfiguration.builder()
                            .apiCallTimeout(Duration.ofSeconds(dbTimeoutSeconds))
                            .apiCallAttemptTimeout(Duration.ofSeconds(Math.max(1, dbTimeoutSeconds / 2)))
                            .build());

            if (!accessKey.isBlank() && !secretKey.isBlank()) {
                clientBuilder.credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
            } else {
                // Szuka kluczy w zmiennych srodowiskowych / systemowych properties / pliku ~/.aws/credentials
                clientBuilder.credentialsProvider(DefaultCredentialsProvider.create());
            }

            dynamoDbClient = clientBuilder.build();
        } catch (Exception ex) {
            getLogger().severe("[DynamoSync] Nie udalo sie zbudowac klienta DynamoDB: " + ex.getMessage());
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        dataManager = new PlayerDataManager(dynamoDbClient, tableName, executor, getLogger(), debug);

        if (createTable) {
            // Blokujace, ale tylko raz przy starcie serwera.
            try {
                dataManager.createTableIfMissing();
            } catch (Exception ex) {
                getLogger().severe("[DynamoSync] Nie udalo sie sprawdzic/utworzyc tabeli: " + ex.getMessage()
                        + " - sprawdz uprawnienia IAM (dynamodb:DescribeTable, dynamodb:CreateTable).");
            }
        }

        getServer().getPluginManager().registerEvents(new PlayerListener(this, dataManager), this);

        int rejoinCooldown = getConfig().getInt("settings.rejoin-cooldown-seconds", 3);
        int ipJoinCooldown = getConfig().getInt("settings.ip-join-cooldown-seconds", 1);
        if (rejoinCooldown > 0 || ipJoinCooldown > 0) {
            joinGuard = new JoinGuard(getLogger(), rejoinCooldown, ipJoinCooldown, debug);
            getServer().getPluginManager().registerEvents(joinGuard, this);
            // Sprzata stare wpisy co 10 minut, zeby mapy nie rosly bez konca na serwerach z duzym ruchem.
            Bukkit.getScheduler().runTaskTimerAsynchronously(this,
                    () -> joinGuard.cleanupOlderThan(TimeUnit.MINUTES.toMillis(10)), 20L * 600, 20L * 600);
            getLogger().info("[DynamoSync] JoinGuard aktywny (rejoin=" + rejoinCooldown + "s, per-IP=" + ipJoinCooldown + "s).");
        }

        // --- Limit zapytan do bazy per adres IP (przesuwane okno) ---
        boolean rateLimitEnabled = getConfig().getBoolean("settings.rate-limit.enabled", true);
        int rateLimitMax = getConfig().getInt("settings.rate-limit.max-requests", 50);
        int rateLimitWindow = getConfig().getInt("settings.rate-limit.window-seconds", 300);
        if (rateLimitEnabled && rateLimitMax > 0 && rateLimitWindow > 0) {
            ipRateLimiter = new IpRateLimiter(rateLimitMax, rateLimitWindow);
            dataManager.setRateLimiter(ipRateLimiter);
            getServer().getPluginManager().registerEvents(new RateLimitListener(ipRateLimiter, getLogger(), debug), this);
            // Co minute sprzata wygasle wpisy, zeby mapa IP nie rosla bez konca.
            Bukkit.getScheduler().runTaskTimerAsynchronously(this, () -> ipRateLimiter.cleanup(), 20L * 60, 20L * 60);
            getLogger().info("[DynamoSync] Limit zapytan per IP aktywny (" + rateLimitMax + " zapytan / "
                    + rateLimitWindow + " s).");
        } else {
            getLogger().info("[DynamoSync] Limit zapytan per IP wylaczony.");
        }

        PlayerDataCommand cmd = new PlayerDataCommand(this, dataManager);
        getCommand("playerdata").setExecutor(cmd);
        getCommand("playerdata").setTabCompleter(cmd);

        if (autosaveSeconds > 0) {
            long ticks = autosaveSeconds * 20L;
            Bukkit.getScheduler().runTaskTimerAsynchronously(this, () -> {
                // autosaveDirty() zapisuje tylko graczy, ktorzy realnie sie zmienili
                // od ostatniego zapisu (BatchWriteItem), zeby nie placic za WCU za nic.
                dataManager.autosaveDirty();
            }, ticks, ticks);
        }

        getLogger().info("[DynamoSync] Wlaczono. Region: " + region + ", tabela: " + tableName);
    }

    @Override
    public void onDisable() {
        if (dataManager != null) {
            getLogger().info("[DynamoSync] Zapisuje dane wszystkich graczy przed wylaczeniem...");
            dataManager.saveAllBlocking();
        }
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
        getLogger().info("[DynamoSync] Wylaczono.");
    }

    public PlayerDataManager getDataManager() {
        return dataManager;
    }
}

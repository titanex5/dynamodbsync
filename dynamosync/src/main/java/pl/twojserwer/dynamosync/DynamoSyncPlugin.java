package pl.twojserwer.dynamosync;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import pl.twojserwer.dynamosync.commands.PlayerDataCommand;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public final class DynamoSyncPlugin extends JavaPlugin {

    private static DynamoSyncPlugin instance;

    private DynamoDbClient dynamoDbClient;
    private ExecutorService executor;
    private PlayerDataManager dataManager;

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

        // Watki na wywolania sieciowe do AWS - nigdy nie blokujemy glownego watku serwera.
        executor = Executors.newFixedThreadPool(4, r -> {
            Thread t = new Thread(r, "DynamoSync-Worker");
            t.setDaemon(true);
            return t;
        });

        try {
            DynamoDbClientBuilder clientBuilder = DynamoDbClient.builder()
                    .region(Region.of(region))
                    .httpClientBuilder(UrlConnectionHttpClient.builder());

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

        PlayerDataCommand cmd = new PlayerDataCommand(this, dataManager);
        getCommand("playerdata").setExecutor(cmd);
        getCommand("playerdata").setTabCompleter(cmd);

        if (autosaveSeconds > 0) {
            long ticks = autosaveSeconds * 20L;
            Bukkit.getScheduler().runTaskTimerAsynchronously(this, () -> {
                dataManager.getCacheSnapshot().values().forEach(dataManager::save);
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

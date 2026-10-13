package pl.twojserwer.dynamosync;

import io.papermc.paper.event.player.AsyncChatEvent;
import org.bukkit.GameMode;
import org.bukkit.entity.ArmorStand;
import org.bukkit.entity.Entity;
import org.bukkit.entity.LivingEntity;
import org.bukkit.entity.Player;
import org.bukkit.entity.Projectile;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.entity.EntityDamageByEntityEvent;
import org.bukkit.event.entity.EntityDamageEvent;
import org.bukkit.event.entity.EntityDeathEvent;
import org.bukkit.event.player.PlayerFishEvent;

/**
 * Zdarzenia zliczajace dodatkowe statystyki (v1.2.0). Wszystko dzieje sie WYLACZNIE w pamieci (zwiekszenie
 * licznika + flaga dirty) - do DynamoDB leci to razem z autosave/wyjsciem gracza, wiec nawet bardzo czeste
 * zdarzenia (lamanie blokow, obrazenia) nie dodaja ani jednego zapytania do bazy.
 *
 * Kazda grupa statystyk ma przelacznik w config.yml ({@code stats.track-*}). Zabojstwa/smierci PvP obsluguje
 * {@link PlayerListener}, czas gry i dystans - {@link StatsSampler}.
 */
public class StatsListener implements Listener {

    private final PlayerDataManager dataManager;
    private final boolean trackBlocks;
    private final boolean trackDamage;
    private final boolean trackChat;
    private final boolean trackFishing;
    private final boolean ignoreCreative;

    public StatsListener(PlayerDataManager dataManager, boolean trackBlocks, boolean trackDamage,
                         boolean trackChat, boolean trackFishing, boolean ignoreCreative) {
        this.dataManager = dataManager;
        this.trackBlocks = trackBlocks;
        this.trackDamage = trackDamage;
        this.trackChat = trackChat;
        this.trackFishing = trackFishing;
        this.ignoreCreative = ignoreCreative;
    }

    /** Czy akcja tego gracza ma byc zaliczona (spectator nigdy, creative zaleznie od configu). */
    private boolean counts(Player player) {
        GameMode mode = player.getGameMode();
        return mode != GameMode.SPECTATOR && !(ignoreCreative && mode == GameMode.CREATIVE);
    }

    private PlayerData dataOf(Player player) {
        return dataManager.getCached(player.getUniqueId());
    }

    /** Obrazenia w dziesiatych czesciach HP (1 serce = 20). */
    private static long tenths(double hp) {
        return Math.round(hp * 10.0);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockBreak(BlockBreakEvent event) {
        if (!trackBlocks || !counts(event.getPlayer())) {
            return;
        }
        PlayerData data = dataOf(event.getPlayer());
        if (data != null) {
            data.addStat(StatType.BLOCKS_BROKEN, 1);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockPlace(BlockPlaceEvent event) {
        if (!trackBlocks || !counts(event.getPlayer())) {
            return;
        }
        PlayerData data = dataOf(event.getPlayer());
        if (data != null) {
            data.addStat(StatType.BLOCKS_PLACED, 1);
        }
    }

    /** Obrazenia otrzymane (przez gracza) i zadane (przez gracza, takze lukiem/rzutka - zob. {@link #attackerOf}). */
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onDamage(EntityDamageEvent event) {
        if (!trackDamage) {
            return;
        }
        double damage = event.getFinalDamage();
        if (damage <= 0) {
            return;
        }
        Entity victim = event.getEntity();

        if (victim instanceof Player victimPlayer) {
            PlayerData data = dataOf(victimPlayer);
            if (data != null) {
                // Przy MONITOR zdrowie jest jeszcze sprzed ciosu - nie liczymy "nadmiaru" ponad zycie.
                data.addStat(StatType.DAMAGE_TAKEN, tenths(Math.min(damage, victimPlayer.getHealth())));
            }
        }

        if (event instanceof EntityDamageByEntityEvent byEntity
                && victim instanceof LivingEntity living && !(victim instanceof ArmorStand)) {
            Player attacker = attackerOf(byEntity.getDamager());
            if (attacker != null && !attacker.getUniqueId().equals(victim.getUniqueId()) && counts(attacker)) {
                PlayerData data = dataOf(attacker);
                if (data != null) {
                    data.addStat(StatType.DAMAGE_DEALT, tenths(Math.min(damage, living.getHealth())));
                }
            }
        }
    }

    /** Zabite moby (gracze maja wlasne statystyki PvP w PlayerListener). */
    @EventHandler(priority = EventPriority.MONITOR)
    public void onEntityDeath(EntityDeathEvent event) {
        if (!trackDamage) {
            return;
        }
        LivingEntity dead = event.getEntity();
        if (dead instanceof Player || dead instanceof ArmorStand) {
            return;
        }
        Player killer = dead.getKiller();
        if (killer == null || !counts(killer)) {
            return;
        }
        PlayerData data = dataOf(killer);
        if (data != null) {
            data.addStat(StatType.MOB_KILLS, 1);
        }
    }

    /** Czat jest asynchroniczny - PlayerData i cache sa bezpieczne watkowo. */
    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onChat(AsyncChatEvent event) {
        if (!trackChat) {
            return;
        }
        PlayerData data = dataOf(event.getPlayer());
        if (data != null) {
            data.addStat(StatType.MESSAGES_SENT, 1);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onFish(PlayerFishEvent event) {
        if (!trackFishing || event.getState() != PlayerFishEvent.State.CAUGHT_FISH || !counts(event.getPlayer())) {
            return;
        }
        PlayerData data = dataOf(event.getPlayer());
        if (data != null) {
            data.addStat(StatType.FISH_CAUGHT, 1);
        }
    }

    /** Gracz, ktory wywolal obrazenia: bezposrednio albo przez pocisk (luk, kusza, trojzab, sniezka...). */
    private static Player attackerOf(Entity damager) {
        if (damager instanceof Player player) {
            return player;
        }
        if (damager instanceof Projectile projectile && projectile.getShooter() instanceof Player shooter) {
            return shooter;
        }
        return null;
    }
}

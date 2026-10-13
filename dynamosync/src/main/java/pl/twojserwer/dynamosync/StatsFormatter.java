package pl.twojserwer.dynamosync;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Wspolne formatowanie statystyk gracza dla komend /stats i /playerdata stats.
 * Zwraca gotowe linie (z kodami kolorow), ktore wolajacy wysyla na czat.
 */
public final class StatsFormatter {

    private StatsFormatter() {
    }

    public static List<String> lines(String displayName, PlayerData d) {
        List<String> out = new ArrayList<>();
        out.add("§7=== Profil: " + InputValidator.escapeForChat(displayName) + " §7===");
        out.add("§eUtworzono: §f" + d.getCreatedDateFormatted() + " §7| §eOstatnio widziany: §f" + d.getLastSeenFormatted());
        out.add("§eCzas gry: §f" + duration(d.getStat(StatType.PLAYTIME))
                + " §7| §eWejsc: §f" + d.getStat(StatType.JOINS)
                + " §7| §eNajdluzsza sesja: §f" + duration(d.getStat(StatType.LONGEST_SESSION)));
        out.add("§ePvP: §f" + d.getStat(StatType.KILLS) + " zabojstw §7| §f" + d.getStat(StatType.DEATHS)
                + " smierci §7| §eK/D: §f" + String.format(Locale.ROOT, "%.2f", d.getKdRatio()));
        out.add("§eNajlepsza seria zabojstw: §f" + d.getStat(StatType.BEST_KILL_STREAK)
                + " §7| §eZabite moby: §f" + d.getStat(StatType.MOB_KILLS));
        out.add("§eBloki: §f" + d.getStat(StatType.BLOCKS_BROKEN) + " wykopanych §7| §f"
                + d.getStat(StatType.BLOCKS_PLACED) + " postawionych");
        out.add("§eObrazenia: §f" + hp(d.getStat(StatType.DAMAGE_DEALT)) + " zadanych §7| §f"
                + hp(d.getStat(StatType.DAMAGE_TAKEN)) + " otrzymanych");
        out.add("§ePrzebyty dystans: §f" + distance(d.getStat(StatType.DISTANCE_CM))
                + " §7| §eWiadomosci: §f" + d.getStat(StatType.MESSAGES_SENT)
                + " §7| §eRyby: §f" + d.getStat(StatType.FISH_CAUGHT));
        Map.Entry<String, Long> topCause = d.getTopDeathCause();
        if (topCause != null) {
            out.add("§eNajczestsza przyczyna smierci: §f" + prettyCause(topCause.getKey())
                    + " §7(" + topCause.getValue() + "x)");
        }
        return out;
    }

    /** 93784 s -> "1 d 2 h 3 min"; krotsze czasy bez zerowych jednostek wiodacych. */
    public static String duration(long seconds) {
        long s = Math.max(0L, seconds);
        long days = s / 86_400L;
        long hours = (s % 86_400L) / 3_600L;
        long minutes = (s % 3_600L) / 60L;
        if (days > 0) {
            return days + " d " + hours + " h " + minutes + " min";
        }
        if (hours > 0) {
            return hours + " h " + minutes + " min";
        }
        return minutes + " min";
    }

    /** Dziesiate czesci HP -> "123.4 HP". */
    public static String hp(long tenths) {
        return String.format(Locale.ROOT, "%.1f HP", tenths / 10.0);
    }

    /** Centymetry -> "12.34 km" albo "540 m". */
    public static String distance(long cm) {
        double meters = Math.max(0L, cm) / 100.0;
        if (meters >= 1000.0) {
            return String.format(Locale.ROOT, "%.2f km", meters / 1000.0);
        }
        return String.format(Locale.ROOT, "%.0f m", meters);
    }

    /** ENTITY_EXPLOSION -> "entity explosion". */
    public static String prettyCause(String cause) {
        return cause.toLowerCase(Locale.ROOT).replace('_', ' ');
    }
}

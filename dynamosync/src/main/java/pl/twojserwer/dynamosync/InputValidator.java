package pl.twojserwer.dynamosync;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Jedno miejsce na cala walidacje i sanityzacje danych wejsciowych.
 *
 * Dane z komend, z API dla innych pluginow oraz z samej bazy (ktos moze je zmienic recznie
 * w AWS Console) traktujemy jako NIEZAUFANE. Wszystkie funkcje tu sa czyste (bez I/O),
 * wiec tanie i bezpieczne do wolania z dowolnego watku.
 */
public final class InputValidator {

    public static final int MAX_CUSTOM_KEY_LENGTH = 64;
    public static final int MAX_CUSTOM_VALUE_LENGTH = 1024;
    public static final int MAX_CUSTOM_FIELDS = 100;
    public static final int MAX_USERNAME_LENGTH = 32;

    // Klucze custom: litery, cyfry, kropka, podkreslnik, myslnik. Bez spacji/znakow kontrolnych.
    private static final Pattern CUSTOM_KEY_PATTERN =
            Pattern.compile("^[A-Za-z0-9_.\\-]{1,%d}$".formatted(MAX_CUSTOM_KEY_LENGTH));

    // Nick Java (3-16 znakow [A-Za-z0-9_]) + opcjonalny jednoznakowy prefiks Floodgate/Bedrock (np. ".").
    // Celowo ostrzejsze niz "cokolwiek": nick trafia do Bukkit.getOfflinePlayer*, a wejscie spoza
    // tego wzorca nie ma prawa byc prawdziwym nickiem.
    private static final Pattern PLAYER_NAME_PATTERN = Pattern.compile("^[.*+!\\-]?[A-Za-z0-9_]{1,16}$");

    // Nazwy pol tabeli "Profiles" - nie wolno ich nadpisac przez mape custom "data".
    // Obejmuje tez atrybuty statystyk z {@link StatType} oraz mape przyczyn smierci.
    private static final Set<String> RESERVED_FIELD_NAMES = buildReservedNames();

    private static Set<String> buildReservedNames() {
        java.util.HashSet<String> names = new java.util.HashSet<>(Set.of(
                "uuid", "username", "createddate", "lastseen", "data", "deathcauses"));
        names.addAll(StatType.reservedAttributeNames());
        return Set.copyOf(names);
    }

    private InputValidator() {
    }

    /** @return null jesli klucz jest OK, w przeciwnym razie komunikat bledu. */
    public static String validateCustomKey(String key) {
        if (key == null || key.isBlank()) {
            return "Klucz nie moze byc pusty.";
        }
        if (!CUSTOM_KEY_PATTERN.matcher(key).matches()) {
            return "Nieprawidlowy klucz (dozwolone: litery, cyfry, '.', '_', '-', max "
                    + MAX_CUSTOM_KEY_LENGTH + " znakow).";
        }
        if (RESERVED_FIELD_NAMES.contains(key.toLowerCase(java.util.Locale.ROOT))) {
            return "Ta nazwa jest zarezerwowana przez schemat tabeli Profiles i nie moze byc uzyta jako klucz custom.";
        }
        return null;
    }

    /** @return null jesli wartosc jest OK, w przeciwnym razie komunikat bledu. */
    public static String validateCustomValue(String value) {
        if (value == null) {
            return "Wartosc nie moze byc null.";
        }
        if (value.length() > MAX_CUSTOM_VALUE_LENGTH) {
            return "Wartosc za dluga (max " + MAX_CUSTOM_VALUE_LENGTH + " znakow, podano " + value.length() + ").";
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            // Znaki kontrolne psuja logi/odczyt w innych miejscach (dozwolone tylko \n i \t).
            if (Character.isISOControl(c) && c != '\n' && c != '\t') {
                return "Wartosc zawiera niedozwolony znak kontrolny.";
            }
            // Samotny surogat to niepoprawny Unicode - serializacja do UTF-8 po cichu zamienilaby go na '?'.
            if (Character.isHighSurrogate(c)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(i + 1))) {
                    return "Wartosc zawiera niepoprawny znak Unicode.";
                }
                i++;
            } else if (Character.isLowSurrogate(c)) {
                return "Wartosc zawiera niepoprawny znak Unicode.";
            }
        }
        return null;
    }

    /** Klucz + wartosc razem. */
    public static String validateCustomField(String key, String value) {
        String keyError = validateCustomKey(key);
        return keyError != null ? keyError : validateCustomValue(value);
    }

    /** Czy to wyglada jak prawdziwy nick gracza (zob. PLAYER_NAME_PATTERN). */
    public static boolean isValidPlayerName(String name) {
        return name != null && PLAYER_NAME_PATTERN.matcher(name).matches();
    }

    /**
     * Czyści nick przed zapisem do bazy/logow: wycina znaki kontrolne i '§', przycina do
     * {@link #MAX_USERNAME_LENGTH}. Prawdziwe nicki Minecrafta sa nietkniete.
     */
    public static String sanitizeUsername(String raw) {
        if (raw == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder(Math.min(raw.length(), MAX_USERNAME_LENGTH));
        for (int i = 0; i < raw.length() && sb.length() < MAX_USERNAME_LENGTH; i++) {
            char c = raw.charAt(i);
            if (Character.isISOControl(c) || c == '\u00A7' || Character.isSurrogate(c)) {
                continue;
            }
            sb.append(c);
        }
        return sb.toString();
    }

    /** Tekst do logow konsoli: bez znakow kontrolnych (ochrona przed log-injection) i przyciety. */
    public static String sanitizeForLog(String raw, int maxLength) {
        if (raw == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < raw.length() && sb.length() < maxLength; i++) {
            char c = raw.charAt(i);
            sb.append(Character.isISOControl(c) ? '?' : c);
        }
        if (raw.length() > maxLength) {
            sb.append("...");
        }
        return sb.toString();
    }

    /**
     * Wartosc z bazy pokazywana adminowi na czacie: '§' zamieniamy na '?', zeby zapisana
     * wartosc nie mogla podszyc sie pod kolorowane/falszywe komunikaty pluginu.
     */
    public static String escapeForChat(String raw) {
        return raw == null ? "" : raw.replace('\u00A7', '?');
    }
}

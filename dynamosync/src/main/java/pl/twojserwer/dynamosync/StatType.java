package pl.twojserwer.dynamosync;

import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Lista statystyk gracza trzymanych jako liczby w rekordzie DynamoDB (kazda = osobny atrybut typu N).
 *
 * Dwa rodzaje:
 *  - {@link Kind#SUM}: licznik narastajacy (zabojstwa, bloki...). Przy odzyskiwaniu po awarii bazy
 *    wartosci z sesji i z bazy sa DODAWANE.
 *  - {@link Kind#MAX}: rekord (najlepsza seria, najdluzsza sesja). Przy odzyskiwaniu bierzemy WIEKSZA wartosc.
 *
 * Nazwy atrybutow {@code playtime}, {@code kills}, {@code deaths} sa takie same jak w v1.1.0, wiec stare
 * rekordy czytaja sie bez migracji; nowe atrybuty w starych rekordach po prostu maja wartosc 0.
 */
public enum StatType {
    PLAYTIME("playtime", Kind.SUM),            // sekundy
    KILLS("kills", Kind.SUM),                  // zabojstwa PvP
    DEATHS("deaths", Kind.SUM),
    MOB_KILLS("mobKills", Kind.SUM),           // zabite moby (nie gracze)
    BLOCKS_BROKEN("blocksBroken", Kind.SUM),
    BLOCKS_PLACED("blocksPlaced", Kind.SUM),
    DAMAGE_DEALT("damageDealt", Kind.SUM),     // dziesiate czesci HP (1 serce = 20)
    DAMAGE_TAKEN("damageTaken", Kind.SUM),     // dziesiate czesci HP
    MESSAGES_SENT("messagesSent", Kind.SUM),
    FISH_CAUGHT("fishCaught", Kind.SUM),
    DISTANCE_CM("distanceCm", Kind.SUM),       // przebyty dystans w centymetrach (statystyki vanilla, delty)
    JOINS("joins", Kind.SUM),                  // liczba wejsc na serwer
    BEST_KILL_STREAK("bestKillStreak", Kind.MAX),
    LONGEST_SESSION("longestSession", Kind.MAX); // sekundy

    public enum Kind { SUM, MAX }

    private final String attribute;
    private final Kind kind;

    StatType(String attribute, Kind kind) {
        this.attribute = attribute;
        this.kind = kind;
    }

    /** Nazwa atrybutu w DynamoDB. */
    public String attribute() {
        return attribute;
    }

    public Kind kind() {
        return kind;
    }

    /** Wszystkie nazwy atrybutow (lowercase) - zarezerwowane, zeby pola custom ich nie przeslanialy. */
    public static Set<String> reservedAttributeNames() {
        return Stream.of(values()).map(t -> t.attribute.toLowerCase(Locale.ROOT)).collect(Collectors.toUnmodifiableSet());
    }
}

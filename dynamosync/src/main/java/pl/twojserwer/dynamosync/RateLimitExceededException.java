package pl.twojserwer.dynamosync;

/**
 * Zwracana (jako wyjatek w CompletableFuture), gdy adres IP przekroczyl limit
 * zapytan do bazy danych. Nie ma stack trace - to zwykly, spodziewany przypadek,
 * a nie blad, wiec nie ma po co go zbierac.
 */
public final class RateLimitExceededException extends RuntimeException {

    private final long retryAfterMillis;

    public RateLimitExceededException(long retryAfterMillis) {
        super("Przekroczono limit zapytan do bazy danych dla tego adresu IP.", null, false, false);
        this.retryAfterMillis = retryAfterMillis;
    }

    public long getRetryAfterMillis() {
        return retryAfterMillis;
    }

    public long getRetryAfterSeconds() {
        return (retryAfterMillis + 999) / 1000;
    }
}

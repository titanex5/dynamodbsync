# DynamoSync — plugin Purpur/Paper ↔ AWS DynamoDB

Zapisuje dane graczy (UUID, nick, pierwsze/ostatnie wejście, czas gry, dowolne pola
custom) w tabeli DynamoDB w regionie `eu-central-1`. Zapis/odczyt zawsze idzie
asynchronicznie, więc lagi sieci OVH↔AWS nie zamrażają serwera.

## 1. Zbuduj plugin

Sandbox, w którym to powstało, nie ma dostępu do repozytoriów Mavena (Maven
Central, repo.papermc.io), więc jar trzeba zbudować u siebie albo — prościej —
przez dołączony workflow GitHub Actions (`.github/workflows/build.yml`), który
buduje go w chmurze i wystawia jako gotowy plik do pobrania w zakładce Actions
→ Artifacts, bez instalowania czegokolwiek lokalnie.

Jeśli wolisz zbudować lokalnie (wymaga JDK 25, Gradle instaluje się sam
poprzez wrapper — a jeśli nie masz `gradlew` w projekcie, po prostu użyj
zainstalowanego globalnie Gradle):

```bash
cd dynamosync
gradle shadowJar
```

Gotowy jar: `build/libs/DynamoSync-1.1.0.jar` → wrzuć do `plugins/` na serwerze.

## 2. Skonfiguruj AWS (konto, IAM, tabela)

Twój serwer stoi na OVH, nie na AWS — nie ma więc roli EC2/instance profile.
Najprościej i najbezpieczniej:

1. W AWS IAM utwórz **osobnego użytkownika programistycznego** (np. `mc-dynamosync`)
   z minimalnymi uprawnieniami — tylko do jednej tabeli:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:BatchWriteItem",
      "dynamodb:DescribeTable",
      "dynamodb:CreateTable"
    ],
    "Resource": "arn:aws:dynamodb:eu-central-1:TWOJE_ID_KONTA:table/Profiles"
  }]
}
```

   (Po pierwszym uruchomieniu, gdy tabela już istnieje, możesz spokojnie usunąć
   `dynamodb:CreateTable` z polityki.)

   > **Uwaga (v1.1.0):** autosave i zapis przy wyłączeniu używają `BatchWriteItem` — wcześniejsza
   > wersja tej polityki go nie zawierała, więc autosave dostawał `AccessDenied`. Dodaj
   > `dynamodb:BatchWriteItem`, jeśli kopiowałeś starą politykę. Celowo **brak** `Scan`, `Query`,
   > `DeleteItem`, `UpdateItem`, `DeleteTable` — nawet wyciek kluczy nie pozwoli wtedy skasować
   > tabeli ani wylistować wszystkich graczy.

2. Wygeneruj klucz dostępu (Access key) dla tego użytkownika.
3. **Nie wpisuj kluczy do `config.yml` na produkcji.** Zamiast tego ustaw je jako
   zmienne środowiskowe procesu Java, którym startujesz serwer:

```bash
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
java -jar purpur-26.2.jar
```

   Plugin automatycznie ich użyje (`DefaultCredentialsProvider`), jeśli pola
   `aws.access-key` / `aws.secret-key` w `config.yml` zostawisz puste — to jest
   pole `access-key: ""` domyślnie w configu.

4. Tabela `player_data` (billing mode `PAY_PER_REQUEST`, partition key `uuid`)
   tworzy się sama przy pierwszym starcie pluginu, jeśli `settings.create-table-if-missing: true`.

## 3. Konfiguracja pluginu (`plugins/DynamoSync/config.yml`)

```yaml
aws:
  region: eu-central-1
  table-name: player_data
  access-key: ""
  secret-key: ""
settings:
  create-table-if-missing: true
  autosave-interval-seconds: 300
  debug: false
```

## 4. Co plugin robi

- **Join**: asynchronicznie ładuje dane gracza z DynamoDB (albo tworzy nowy rekord),
  aktualizuje nick i `lastJoin`.
- **Quit**: dolicza czas sesji do `playtimeSeconds` i zapisuje cały rekord do DynamoDB.
- **Autosave**: co `autosave-interval-seconds` (domyślnie 5 min) zapisuje dane
  wszystkich graczy aktualnie online — zabezpieczenie na wypadek crasha serwera.
- **Shutdown**: przy `/stop` zapisuje wszystkich graczy jeszcze raz, blokująco,
  zanim serwer się zamknie.
- **Komenda** `/playerdata get|set <gracz> <klucz> [wartość]` oraz `/playerdata stats <gracz>`
  (uprawnienie `dynamosync.admin`, domyślnie tylko OP) — do ręcznego podglądu/edycji pól
  custom i podglądu statystyk PvP.
- **Statystyki PvP**: plugin nasłuchuje na śmierci graczy. Ofiara dostaje +1 do `deaths`,
  a jeśli była to śmierć z ręki innego gracza (nie środowiska/moba) — zabójca dostaje +1
  do `kills`. Statystyki liczą się w pamięci i lecą do DynamoDB przy najbliższym autosave
  albo przy wyjściu gracza (żeby nie zapychać API przy intensywnym PvP zapisem po każdym fragu).

## 4a. Limit zapytań do bazy per adres IP

Z jednego adresu IP można wykonać maksymalnie **50 zapytań do DynamoDB w ciągu 5 minut**
(przesuwane okno, nie sztywne przedziały). Wartości w `config.yml`:

```yaml
settings:
  rate-limit:
    enabled: true
    max-requests: 50
    window-seconds: 300
```

Co liczy się jako zapytanie: odczyt przy wejściu gracza (`GetItem`), odczyty/zapisy z komendy
`/playerdata` oraz zapisy danych gracza (wyjście, autosave, shutdown). Trafienie w cache **nie**
jest zapytaniem do bazy i nie zużywa limitu.

Po wyczerpaniu limitu:

- logowanie z tego IP jest odrzucane (`AsyncPlayerPreLoginEvent`, zanim cokolwiek trafi do bazy)
  z komunikatem „spróbuj za X s”,
- `/playerdata get|set|stats` zwraca ten sam komunikat,
- **zapisy danych graczy nigdy nie są odrzucane** — zużywają limit, ale zawsze się wykonują
  (odrzucony zapis oznaczałby utratę postępów gracza),
- konsola serwera nie ma IP, więc nie jest limitowana.

`/playerdata debug` pokazuje aktualną konfigurację limitu, liczbę śledzonych IP i liczbę
odrzuconych zapytań. **Za proxy (Velocity/BungeeCord)** włącz przekazywanie prawdziwych adresów IP,
inaczej wszyscy gracze będą miały adres proxy i dzielili jeden limit.

## 4b. Twardnienie bezpieczeństwa (v1.1.0)

Wszystkie zabezpieczenia są niewidoczne dla zwykłych graczy — dotykają tylko nietypowych / złośliwych
zachowań. Co dodano:

**Integralność danych (najważniejsze)**

- **Nieudany odczyt nie kasuje już profilu.** Gdy `GetItem` się nie powiedzie (throttling, awaria sieci),
  gracz dostaje tymczasowy *profil awaryjny*, który **nigdy nie jest zapisywany**. W tle (po 5/10/20 s,
  potem przy każdym autosave) ponawiamy odczyt i dopiero wtedy łączymy zmiany z sesji (kills, deaths,
  czas gry, pola custom) z prawdziwym rekordem. Wcześniej pusty profil nadpisywał w bazie prawdziwy.
- **`BatchWriteItem` liczy jako zapisane tylko to, co baza potwierdziła.** Reszta zostaje „brudna” i
  idzie przy następnym autosave (wcześniej błędy/throttling po cichu gubiły dane).
- **Zmiana w trakcie zapisu nie ginie** (flaga dirty zdejmowana przed migawką, nie po zapisie).
- **Nieudany zapis przy wyjściu gracza nie usuwa jego danych z pamięci** — dociągnie je autosave.
- Odczyty są **silnie spójne** (`consistentRead`) — przy szybkim wyjściu/wejściu nie nadpiszemy nowszych
  danych starszymi.

**Walidacja wejścia**

- `PlayerData#setCustom` (API dla innych pluginów) waliduje teraz tak samo jak komenda: klucz, długość
  wartości, znaki kontrolne, niepoprawny Unicode, zarezerwowane nazwy i maks. 100 pól. Rzuca
  `IllegalArgumentException` przy złych danych. `getCustomData()` zwraca widok tylko do odczytu.
- Rozmiar itemu liczony w **bajtach UTF-8** (wcześniej w znakach — emoji/polskie znaki mogły przekroczyć 400 KB).
- Dane z bazy traktowane jako niezaufane: zły typ pola, ujemne liczby czy dziwny nick nie wywracają odczytu.

**Limity i odporność na flood**

- Limit per IP liczy **IPv6 per podsieć /64** (inaczej atakujący z IPv6 miał „nowy” limit przy każdym połączeniu).
- Liczba śledzonych adresów jest ograniczona (50 000 / 100 000) — botnet nie zje pamięci. Po przepełnieniu
  nowe adresy są przepuszczane (*fail-open*), żeby nie odciąć prawdziwych graczy.
- `JoinGuard` nie nadpisuje już komunikatów banów/whitelisty, a sprawdzenie cooldownu jest atomowe.
- Timeouty zapytań do DynamoDB (`db-timeout-seconds`) — zawieszona baza nie zablokuje puli wątków.

**Komenda `/playerdata`**

- Nick celu walidowany; cel wyszukiwany tylko wśród graczy znanych serwerowi (bez blokującego zapytania do
  Mojang na głównym wątku). **Zmiana zachowania dla adminów:** nie da się już edytować gracza, który nigdy
  nie wszedł na ten serwer.
- `get`/`stats` nie tworzą pustych profili ani wpisów w cache (literówka w nicku nie zaśmieca bazy).
- **Audyt:** każde `set` trafia do konsoli (`[AUDIT]`), z odfiltrowanymi znakami kontrolnymi (`audit-log` w configu).
- Wartości z bazy pokazywane bez kodów kolorów `§`; tab-complete tylko dla osób z uprawnieniem.

**Infrastruktura**

- Nowo tworzona tabela ma włączoną **ochronę przed usunięciem** (deletion protection).
- Ostrzeżenie w logu, gdy klucze AWS są wpisane w `config.yml`; walidacja nazwy tabeli; minimalny autosave 10 s.
- Workflow GitHub Actions działa z tokenem `contents: read`.

**Zalecane po stronie AWS (poza pluginem):** włącz *Point-in-time recovery* na tabeli (pozwala cofnąć
zniszczone/nadpisane dane) oraz alarm CloudWatch na `ThrottledRequests` / `SystemErrors`.

## 5. Rozbudowa (API dla innych pluginów)

Inny plugin na tym samym serwerze może się podpiąć tak:

```java
PlayerDataManager mgr = DynamoSyncPlugin.getInstance().getDataManager();
PlayerData data = mgr.getCached(player.getUniqueId()); // null, jesli jeszcze nie zaladowane
if (data != null) {
    data.setCustom("ekonomia.saldo", "1500"); // rzuca IllegalArgumentException przy niepoprawnym kluczu/wartosci
    mgr.save(data);
}
```

Rekord w DynamoDB ma pole `data` (mapa string→string) właśnie do tego typu
rozszerzeń — możesz tam trzymać dowolne dodatkowe statystyki bez zmiany schematu.

## 6. Koszty i limity

`PAY_PER_REQUEST` w DynamoDB znaczy płacisz za operacje, nie za czas działania —
dla małego/średniego serwera to zwykle grosze miesięcznie. Jeśli masz duży ruch,
rozważ przejście na tryb `PROVISIONED` z autoscalingiem po analizie kosztów w AWS
Cost Explorer.

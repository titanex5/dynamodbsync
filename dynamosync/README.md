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

Gotowy jar: `build/libs/DynamoSync-1.0.0.jar` → wrzuć do `plugins/` na serwerze.

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
      "dynamodb:DescribeTable",
      "dynamodb:CreateTable"
    ],
    "Resource": "arn:aws:dynamodb:eu-central-1:TWOJE_ID_KONTA:table/Profiles"
  }]
}
```

   (Po pierwszym uruchomieniu, gdy tabela już istnieje, możesz spokojnie usunąć
   `dynamodb:CreateTable` z polityki.)

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

## 5. Rozbudowa (API dla innych pluginów)

Inny plugin na tym samym serwerze może się podpiąć tak:

```java
PlayerDataManager mgr = DynamoSyncPlugin.getInstance().getDataManager();
PlayerData data = mgr.getCached(player.getUniqueId()); // null, jesli jeszcze nie zaladowane
if (data != null) {
    data.setCustom("ekonomia.saldo", "1500");
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

# Cara — Project-Agnostic Primitive Önerileri

> Amaç: Bir consumer uygulamanın **API ve worker katmanlarında** olgunlaşmış ama
> aslında **projeye özel olmayan** birkaç deseni cara'ya çıkarmak. Böylece her
> yeni proje bunları bedavaya alır ve app katmanı incelir.
>
> Bu doküman Akinon (omnitron/channel_app) incelemesinin çıktısıdır. Önemli
> bulgu: Akinon'dan **taşınacak yeni bir mimari desen yok** — outbox, inbox,
> delta/drift, 3 seviye mapping, connector-contract, verify/reconcile hepsi
> incelenen consumer uygulamada zaten var. Aşağıdakiler Akinon'dan değil,
> **kendi kodumuzdaki tekrar/eksikten** çıkan, framework'e ait generic parçalar.

Her öneri şu formatta: **neden** · **şu an app'te nasıl (kanıt)** · **cara API taslağı** · **sınır (cara'ya girMEyecek kısım)**.

---

## 1. `MakesFieldHistory` concern — (en yüksek getiri)

**Neden.** App'te birbirinin neredeyse aynısı **6+ history tablosu** var; hepsi
"bir kolon değişince eski→yeni değeri bir ledger satırına yaz" yapıyor:

- `ListingPriceHistory`
- `ListingQuantityHistory`
- `ListingStatusHistory`
- `ListingIntentHistory`
- `ProductCostHistory`
- `ProductQuantityHistory`

Aynı iskelet 6 kez kopyalanmış. Bu klasik bir framework-concern adayı.

**Şu an app'te.** `ListingWriter` "write-through + history ledger" mantığını elle
yürütüyor (`_write_observed(column=...)` gibi). Her history modeli ayrı migration,
ayrı model, ayrı yazma yolu.

**cara API taslağı.**

```python
class Listing(Model, MakesFieldHistory):
    __tracked_fields__ = {
        # kolon            -> history tablosu + ekstra bağlam kolonları
        "price":    {"table": "listing_price_history",    "context": ["reason"]},
        "quantity": {"table": "listing_quantity_history", "context": ["reason"]},
        "status":   {"table": "listing_status_history"},
    }

# Yazma tek yoldan; concern otomatik ledger satırı basar:
listing.track_set("price", 90.0, reason="reprice.margin_buybox", actor=user_id)
# -> listing.price = 90.0
# -> INSERT listing_price_history {old:100.0, new:90.0, reason, actor, at}
#    (yalnızca değer GERÇEKTEN değiştiyse)
```

Ledger satırının standart şekli: `{old, new, reason, actor_id, at}` + tanımlı
`context` kolonları. Migration üretimi için concern bir yardımcı verebilir
(`MakesFieldHistory.history_schema("listing_price_history", extra=[...])`).

**Sınır.** Hangi kolonların izleneceği ve history tablo adları **app'te** kalır
(domain). cara sadece "izlenen kolon → ledger" mekanizmasını sağlar.

---

## 2. `Outbox` primitive (`cara.outbox`)

**Neden.** `SyncTask` elle yazılmış bir outbox. Deseni generic: dış sisteme her
mutasyon **önce satır** olur, worker gönderir, onaylar; iş asla kaybolmaz, iki kez
gitmez, gözlemlenebilir.

**Şu an app'te.** `SyncTask` (queued→sent→confirmed|failed|cancelled),
`(listing, kind)` başına tek OPEN task (partial-unique), `attempts`/`last_error`,
üstüne `verify_result` (ok/mismatch/unavailable). `PushSyncTaskJob` gönderir,
`VerifySyncTasksJob` doğrular, `SweepStaleSyncTasksJob` bayatları toplar.

**cara API taslağı.** cara bir taban model/concern + worker döngüsü verir:

```python
class SyncTask(Model, MakesOutbox):
    __outbox_open_statuses__ = ("queued", "sent")
    __outbox_dedupe_key__    = ("listing_id", "kind")   # tek-open partial unique
    __outbox_max_attempts__  = 8

# cara sağlar:
#  - open(key, payload)  -> yoksa satır aç, varsa dokunma (dedupe)
#  - mark_sent / mark_confirmed / mark_failed(err) (attempts++/last_error)
#  - verify hook arayüzü (app doğrulama mantığını implemente eder)
#  - partial-unique-open index'i migration'a otomatik ekleme
```

**Sınır.** `kind` sözlüğü (price/inventory/content/publish/end) ve **gerçek
`send()`** (connector çağrısı) app/connector'da kalır. cara yalnızca outbox
yaşam döngüsü + dedupe + verify iskeleti.

---

## 3. `Inbox` / webhook ingest primitive (`cara.inbox`)

**Neden.** `ChannelEvent` elle yazılmış dayanıklı inbox. Deseni generic: gelen
webhook/bildirim **ham haliyle önce persist**, sonra işlenir; tekrar oynatılabilir
(replay), aynı olay iki kez giremez (dedupe).

**Şu an app'te.** `ChannelEvent` — `(channel, event_type, external_id)` üzerinde
partial-unique dedupe (status pending/processed iken), status makinesi
(pending/processed/failed/ignored), ham `payload` saklı.

**cara API taslağı.**

```python
class ChannelEvent(Model, MakesInbox):
    __inbox_dedupe_key__ = ("channel_id", "event_type", "external_id")
    __inbox_statuses__   = ("pending", "processed", "failed", "ignored")

# cara sağlar:
#  - ingest(raw) -> ham payload'ı persist, dedupe'e takılırsa no-op döner
#  - replay(id)  -> tekrar işleme kuyruğa at
#  - dedupe partial-unique index'ini migration'a otomatik ekleme
#  - HTTP tarafı için "persist-first-then-enqueue" yardımcı akışı
```

**Sınır.** `event_type` yönlendirme/işleme mantığı app'te kalır. cara sadece
dayanıklı kabul + dedupe + replay iskeleti.

---

## 4. Idempotency-Key middleware (`cara.http`)

**Neden.** Şu an **inbound olay** dedupe'u var (ChannelEvent), ama **bizim kendi
API'mize** gelen yazma isteklerinde generic bir `Idempotency-Key` mekanizması yok.
Ağ retry'ı ya da çift tık aynı POST'u iki kez işleyebilir (çift sipariş/çift
mutasyon). Stripe tarzı çözüm framework'e ait.

**cara API taslağı.** Bir HTTP middleware:

```
Idempotency-Key: <client-uuid>   # gelen POST/PATCH header'ı

# middleware:
#  - anahtar = (route, tenant_id, header_key)
#  - redis/cache'te varsa: kaydedilmiş yanıtı AYNEN döndür (handler'a hiç girme)
#  - yoksa: handler'ı çalıştır, yanıtı anahtar altında TTL ile sakla
```

Rota bazında opt-in (`middleware=["idempotent"]`). Tenant-scope zaten var, ona
takılır.

**Sınır.** Hangi rotaların idempotent olacağı app config'i. Depolama cara.cache
üstünden.

---

## 5. (Opsiyonel) `MakesExternalIdentity` concern

**Neden.** `Listing` "yerel kayıt ↔ kanal external_id + version_date drift"
iskeletini gömülü taşıyor. Bunun **generic çekirdeği** (kaynak başına external_id
eşlemesi + `version_date` ile kirlilik) tekrar kullanılabilir.

**Sınır — dikkat.** Bu domain'e en yakın olan öneri. Yalnızca "external kimlik +
version_date" iskeleti taşınmalı; **product-linking confidence ladder, intent,
mapping** kesinlikle app'te kalır. Aşırı soyutlama riski yüksek — ilk 4 önerinin
ardından, gerçekten ikinci bir "kanala projekte edilen kayıt" tipi doğarsa yapılsın.

---

## cara'ya GİRMEYECEKLER (net sınır)

Bunlar consumer domain'i, framework değil — app'te kalır:

- Marketplace mapping (attribute/value/category eşleştirme kuralları)
- Product-linking confidence ladder (identifier→sku→manual)
- Repricing (margin_buybox, MarginRepricer)
- Listing intent (desired_price/quantity_override) ve iş kuralları
- Connector'ların domain yüzeyi (push_listing/pull_orders payload şekilleri)

Kural: bir parça **"herhangi bir dış-sistem-senkronu yapan proje bunu ister"**
testini geçiyorsa cara'ya; **"sadece marketplace satıcı ürünü senkronu ister"**
ise app'te.

---

## Öncelik sırası

1. **MakesFieldHistory** — 6 tabloyu DRY eder, düşük risk, hemen kazanç.
2. **Outbox concern** — SyncTask'ı iskelete oturtur, sonraki dış entegrasyonlar bedavaya alır.
3. **Inbox concern** — ChannelEvent'i iskelete oturtur.
4. **Idempotency-Key middleware** — gerçek boşluk, küçük ama kritik.
5. **MakesExternalIdentity** — sadece ikinci bir kullanım doğarsa.

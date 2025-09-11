#include "wifi_post.h"

#include <Arduino.h>
#include <WiFi.h>
#include <HTTPClient.h>
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"

#include "net_time.h"
#include "sdcard.h"   // sdjson_delete_first_lines()

#include <stdio.h>
#include <string.h>

#include <esp_heap_caps.h>
extern "C" {
  #include "esp_system.h"
  #include "esp_bt.h"
}

/* ── PROTOTIPOS DEL LOGGER SD ───────────────────────────────────────────── */
extern bool sdjson_logger_start(void);
extern void sdjson_logger_stop(void);
extern "C" bool sdjson_delete_first_lines(size_t n);
/* Cerrar la línea actual de forma SÍNCRONA antes de postear */
extern "C" bool sdcard_newline_sync(uint32_t timeout_ms);
/* Cerrar la línea actual (asíncrono) */
extern "C" void sdcard_newline(void);
/* APPEND para {t,w} cuando no hay Wi-Fi */
extern "C" void sdcard_append_jsonl(const char *chunk);
/* Solicitar purga de líneas antiguas al writer (seguro y sin carreras) */
extern "C" void sdjson_request_purge_older_than(uint32_t max_age_sec);
/* ───────────────────────────────────────────────────────────────────────── */

#ifndef WIFI_SSID
#warning "WIFI_SSID no definido (-D WIFI_SSID=\"...\")"
#define WIFI_SSID ""
#endif
#ifndef WIFI_PASS
#define WIFI_PASS ""
#endif

#ifndef POST_URL
#define POST_URL "https://plataforma.phebus.net:443/api/v1/YQARkOKOcKThFSGIAWar/telemetry"
#endif

// Umbrales de memoria para intentar TLS
#ifndef TLS_MIN_FREE_HEAP
#define TLS_MIN_FREE_HEAP       38000
#endif
#ifndef TLS_MIN_LARGEST_BLOCK
#define TLS_MIN_LARGEST_BLOCK   24000
#endif

// Tamaño de chunk de streaming (más pequeño = menos pila usada)
#ifndef STREAM_CHUNK_MAX
#define STREAM_CHUNK_MAX  256
#endif

#ifndef MOUNT_POINT
#define MOUNT_POINT "/sdcard"
#endif
#ifndef SDCARD_MACLOG_BASENAME
#define SDCARD_MACLOG_BASENAME "mac_events"
#endif

typedef struct {
  int    wifi;   // recuento a reportar; si es -1 => tick sin recuento (no lo usamos)
  time_t ts;     // timestamp del recuento
} http_msg_t;

static QueueHandle_t gWifiHttpQueue = nullptr;
static TaskHandle_t  gWifiHttpTask  = nullptr;
static TaskHandle_t  gPurgeTask     = nullptr;

/* Estado de sesión Wi-Fi: queremos 1 solo POST por conexión */
static bool s_wasConnected = false;
static bool s_flushedThisConnection = false;

/* ────────────────────────────────────────────────────────────────────────── */

static void log_mem(const char* tag) {
  size_t freeHeap   = heap_caps_get_free_size(MALLOC_CAP_DEFAULT);
  size_t minFree    = heap_caps_get_minimum_free_size(MALLOC_CAP_DEFAULT);
  size_t largestBlk = heap_caps_get_largest_free_block(MALLOC_CAP_DEFAULT);
  Serial.printf("[MEM] %s free=%u min=%u largest=%u\n",
                tag, (unsigned)freeHeap, (unsigned)minFree, (unsigned)largestBlk);
}

static void log_stack_watermark(const char* tag) {
  UBaseType_t hw = uxTaskGetStackHighWaterMark(nullptr);
  Serial.printf("[STACK] %s watermark=%u bytes\n", tag, (unsigned)(hw * sizeof(StackType_t)));
}

// Conexión con timeout y logs claros (devuelve true si quedó conectado)
static bool connectToWiFi_local(uint32_t timeoutMs = 15000) {
  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("[WIFI] Ya conectado. IP: %s\n", WiFi.localIP().toString().c_str());
    return true;
  }
  WiFi.mode(WIFI_STA);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  Serial.println("[WIFI] Conectando a Wi-Fi (HTTP)...");
  uint32_t start = millis(), lastDot = 0;

  while (WiFi.status() != WL_CONNECTED && (millis() - start) < timeoutMs) {
    if (millis() - lastDot >= 500) { Serial.print("."); lastDot = millis(); }
    vTaskDelay(pdMS_TO_TICKS(50));
  }

  if (WiFi.status() == WL_CONNECTED) {
    Serial.printf("\n[WIFI] Conectado. IP: %s\n", WiFi.localIP().toString().c_str());
    return true;
  } else {
    Serial.println("\n[WIFI] ERROR: no se pudo conectar (timeout).");
    return false;
  }
}

static inline bool have_tls_memory() {
  size_t freeHeap   = heap_caps_get_free_size(MALLOC_CAP_DEFAULT);
  size_t largestBlk = heap_caps_get_largest_free_block(MALLOC_CAP_DEFAULT);
  return (freeHeap >= TLS_MIN_FREE_HEAP) && (largestBlk >= TLS_MIN_LARGEST_BLOCK);
}

/* ──────────────────────────────────────────────────────────────────────────
   Calcular tamaño total para Content-Length del streaming.
   Cada línea del archivo es un "lote" de objetos (separados por comas).
   ────────────────────────────────────────────────────────────────────────── */
struct NdjsonStats {
  size_t lines = 0;  // nº de líneas (lotes)
  size_t bytes = 0;  // suma de longitudes de líneas, sin CR/LF
};

static bool compute_ndjson_stats(const char* path, NdjsonStats& st) {
  FILE* f = fopen(path, "r");
  if (!f) {
    st.lines = 0;
    st.bytes = 0;
    return false;
  }

  size_t lines = 0, bytes = 0, cur = 0;
  int c;
  while ((c = fgetc(f)) != EOF) {
    if (c == '\r') continue;
    if (c == '\n') {
      if (cur > 0) { lines++; bytes += cur; cur = 0; }
    } else {
      cur++;
    }
  }
  if (cur > 0) { lines++; bytes += cur; }
  fclose(f);

  st.lines = lines;
  st.bytes = bytes;
  return true;
}

/* Stream que emite: prefix + (linea1 + , + linea2 + ...) + suffix sin cargar todo en RAM */
class NdjsonArrayStream : public Stream {
public:
  NdjsonArrayStream(const char* path,
                    const char* prefix, size_t prefix_len,
                    const char* suffix, size_t suffix_len)
  : _f(nullptr),
    _path(path),
    _prefix(prefix), _prefix_len(prefix_len), _prefix_pos(0),
    _suffix(suffix), _suffix_len(suffix_len), _suffix_pos(0),
    _state(STATE_PREFIX),
    _need_comma_between_lines(false),
    _in_line(false),
    _line_started(false),
    _buf_len(0), _buf_pos(0)
  {}

  bool begin() {
    _f = fopen(_path, "r");
    _buf_len = _buf_pos = 0;
    _need_comma_between_lines = false;
    _in_line = false;
    _line_started = false;
    _state = STATE_PREFIX;
    return true; // incluso si _f == nullptr, enviaremos prefix+suffix
  }

  void end() {
    if (_f) { fclose(_f); _f = nullptr; }
  }

  int available() override {
    if (_buf_pos < _buf_len) return (int)(_buf_len - _buf_pos);

    if (_state == STATE_PREFIX) {
      if (_prefix_pos < _prefix_len) {
        size_t remain = _prefix_len - _prefix_pos;
        size_t n = (remain > STREAM_CHUNK_MAX) ? STREAM_CHUNK_MAX : remain;
        memcpy(_buf, _prefix + _prefix_pos, n);
        _prefix_pos += n;
        _buf_len = n; _buf_pos = 0;
        return (int)n;
      } else {
        _state = STATE_FILE;
      }
    }

    if (_state == STATE_FILE) {
      int n = fill_from_file();
      if (n > 0) return n;
      _state = STATE_SUFFIX;
    }

    if (_state == STATE_SUFFIX) {
      if (_suffix_pos < _suffix_len) {
        size_t remain = _suffix_len - _suffix_pos;
        size_t n = (remain > STREAM_CHUNK_MAX) ? STREAM_CHUNK_MAX : remain;
        memcpy(_buf, _suffix + _suffix_pos, n);
        _suffix_pos += n;
        _buf_len = n; _buf_pos = 0;
        return (int)n;
      } else {
        _state = STATE_DONE;
      }
    }

    return 0;
  }

  int read() override {
    if (available() <= 0) return -1;
    return _buf[_buf_pos++];
  }

  int peek() override {
    if (available() <= 0) return -1;
    return _buf[_buf_pos];
  }

  void flush() override {}
  size_t write(uint8_t) override { return 0; }

private:
  enum State { STATE_PREFIX, STATE_FILE, STATE_SUFFIX, STATE_DONE };

  int fill_from_file() {
    _buf_len = _buf_pos = 0;

    if (!_f) return 0; // no hay archivo, no hay líneas

    while (_buf_len < sizeof(_buf)) {
      int c = fgetc(_f);
      if (c == EOF) {
        if (_in_line && _line_started) {
          _in_line = false;
          _need_comma_between_lines = true;
        }
        break;
      }
      if (c == '\r') continue;

      if (!_in_line) {
        if (c == '\n') continue; // saltar líneas vacías
        // Comienza línea nueva con contenido
        _in_line = true;
        _line_started = false;
        if (_need_comma_between_lines) {
          if (_buf_len < sizeof(_buf)) _buf[_buf_len++] = ',';
          else { ungetc(c, _f); break; }
          _need_comma_between_lines = false;
        }
      }

      if (c == '\n') {
        _in_line = false;
        _need_comma_between_lines = true;
        if (_buf_len == 0) continue;
        else break;
      } else {
        if (_buf_len < sizeof(_buf)) {
          _buf[_buf_len++] = (uint8_t)c;
          _line_started = true;
        } else {
          break;
        }
      }
    }
    return (int)_buf_len;
  }

  FILE*  _f;
  const char* _path;

  const char* _prefix; size_t _prefix_len; size_t _prefix_pos;
  const char* _suffix; size_t _suffix_len; size_t _suffix_pos;

  State  _state;

  bool   _need_comma_between_lines;
  bool   _in_line;
  bool   _line_started;

  uint8_t _buf[STREAM_CHUNK_MAX];
  size_t  _buf_len, _buf_pos;
};

/* ────────────────────────────────────────────────────────────────────────── */

static bool post_all_in_one_stream(int wifiCount, time_t ts, size_t& out_lines_sent) {
  out_lines_sent = 0;

  // Paramos el writer para fijar el archivo durante el envío
  sdjson_logger_stop();

  char fullpath[96];
  snprintf(fullpath, sizeof(fullpath), "%s/%s.jsonl", MOUNT_POINT, SDCARD_MACLOG_BASENAME);

  // Estadísticas del NDJSON actual (no exigimos '\n' final)
  NdjsonStats st = {};
  (void)compute_ndjson_stats(fullpath, st); // si no existe, st.lines=0

  // Prefijo del JSON a enviar
  char prefix[128];
  snprintf(prefix, sizeof(prefix),
           "{\"recuento_max\":%d,\"ts\":%lu,\"events\":[",
           (wifiCount < 0 ? 0 : wifiCount), (unsigned long)ts);
  const size_t prefix_len = strnlen(prefix, sizeof(prefix));

  static const char suffix[] = "]}";
  const size_t suffix_len = sizeof(suffix) - 1;

  // Separadores entre líneas
  size_t commas = (st.lines > 0) ? (st.lines - 1) : 0;
  size_t content_len = prefix_len + st.bytes + commas + suffix_len;

  log_mem("antes POST");
  log_stack_watermark("antes POST");

  if (!have_tls_memory()) {
    size_t freeHeap   = heap_caps_get_free_size(MALLOC_CAP_DEFAULT);
    size_t largestBlk = heap_caps_get_largest_free_block(MALLOC_CAP_DEFAULT);
    Serial.printf("[HTTP] Heap insuficiente para TLS (free=%u, largest=%u) → salto envío\n",
                  (unsigned)freeHeap, (unsigned)largestBlk);
    // El writer se re-arranca fuera (wifi_http_task)
    return false;
  }

  WiFiClientSecure client;
  client.setInsecure();
  HTTPClient http;
  http.setTimeout(8000);

  if (!http.begin(client, POST_URL)) {
    Serial.println("[HTTP] begin() falló");
    return false;
  }
  http.addHeader("Content-Type", "application/json");

  NdjsonArrayStream streamer(fullpath, prefix, prefix_len, suffix, suffix_len);
  streamer.begin();

  int code = http.sendRequest("POST", &streamer, content_len);

  streamer.end();

  bool ok = (code > 0 && code < 400);
  if (ok) {
    Serial.printf("[HTTP] POST OK (%d)  (lines=%u bytes=%u)\n",
                  code, (unsigned)st.lines, (unsigned)st.bytes);
    out_lines_sent = st.lines;
  } else {
    Serial.printf("[HTTP] POST FAIL (%d)\n", code);
  }

  http.end();
  log_mem("despues POST");
  log_stack_watermark("despues POST");

  return ok;
}

static void wifi_http_task(void *pvParameters) {
  (void) pvParameters;

  // intento inicial de conexión (con logs)
  (void)connectToWiFi_local();
  netTimeInit();

  http_msg_t m;

  for (;;) {
    // Espera *solo* a que llegue un recuento real
    if (xQueueReceive(gWifiHttpQueue, &m, portMAX_DELAY) != pdTRUE)
      continue;

    // Observa transición de estado Wi-Fi
    bool nowConnected = (WiFi.status() == WL_CONNECTED);
    if (!nowConnected) (void)connectToWiFi_local(3000);
    nowConnected = (WiFi.status() == WL_CONNECTED);

    // Si acabamos de conectar (antes no lo estábamos) -> permite flush único
    if (nowConnected && !s_wasConnected) {
      s_flushedThisConnection = false;
      Serial.println("[WIFI] Conectado: listo para 1 solo POST de vaciado.");
    }
    // Si nos hemos caído, resetea bandera para la próxima conexión
    if (!nowConnected && s_wasConnected) {
      s_flushedThisConnection = false;
      Serial.println("[WIFI] Desconectado: se permitirá un POST cuando reconecte.");
    }
    s_wasConnected = nowConnected;

    // Si NO hay Wi-Fi ahora mismo: añadimos {t,w} y SELLAMOS el lote (simula intento de POST fallido)
    if (!nowConnected) {
      char line[48];
      snprintf(line, sizeof(line), "{\"t\":%lu,\"w\":%d}",
               (unsigned long)m.ts, m.wifi);
      sdcard_append_jsonl(line); // añade a la línea actual
      sdcard_newline();          // salto de línea SOLO aquí (sin Wi-Fi = “intento” fallido)
      Serial.println("[HTTP] Sin Wi-Fi: recuento guardado y lote sellado (equivalente a intento de POST).");
      continue;
    }

    // Ya hay Wi-Fi: solo permitimos 1 POST por conexión
    if (s_flushedThisConnection) {
      Serial.println("[HTTP] Ya se vació en esta conexión; no se postea de nuevo.");
      continue;
    }

    // Hacemos el POST único: envía TODO lo que haya en la SD en ESTE instante
    size_t sent = 0;
    bool ok = post_all_in_one_stream(m.wifi, m.ts, sent);

    if (ok && sent > 0) {
      // Borrar líneas enviadas con el writer aún detenido
      if (!sdjson_delete_first_lines(sent)) {
        Serial.printf("[HTTP] WARNING: no pude borrar %u lineas del backlog\n",
                      (unsigned)sent);
      }
    } else {
      // Intento de POST fallido: SELLAMOS lote para marcar el corte
      Serial.println("[HTTP] POST FAIL -> lote sellado; no se borra backlog.");
      sdcard_newline();  // salto de línea SOLO cuando el POST falla
    }

    // Re-arrancamos el writer aquí (después de borrar o decidir no borrar)
    sdjson_logger_start();

    // Marcar que ya vaciamos en esta conexión
    s_flushedThisConnection = true;

    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

/* ── TAREA PERIÓDICA: purga cada 6 minutos cuando no hay Internet ───────── */

static void backlog_purge_task(void *pvParameters) {
  (void)pvParameters;
  const uint32_t kIntervalMs = 6 * 60 * 1000; // 6 minutos
  const uint32_t kMaxAgeSecs = 24 * 60 * 60;  // 24 horas

  for (;;) {
    vTaskDelay(pdMS_TO_TICKS(kIntervalMs));

    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[PURGE] Offline: solicitando purga de líneas > 24h...");
      // Lo hace la tarea writer, sin carreras
      sdjson_request_purge_older_than(kMaxAgeSecs);
    }
  }
}

/* ────────────────────────────────────────────────────────────────────────── */

void wifi_post_init(void) {
  // Libera RAM del stack BT si no se usa
  esp_bt_controller_mem_release(ESP_BT_MODE_BTDM);

  if (!gWifiHttpQueue)
    gWifiHttpQueue = xQueueCreate(8, sizeof(http_msg_t));
  if (!gWifiHttpTask) {
    // Subimos el stack para evitar “Double exception”
    xTaskCreatePinnedToCore(
      wifi_http_task, "wifi_http_task", 12288, NULL, 1, &gWifiHttpTask, 1
    );
  }
  if (!gPurgeTask) {
    xTaskCreatePinnedToCore(
      backlog_purge_task, "sd_purge_task", 4096, NULL, 1, &gPurgeTask, 1
    );
  }
}

void wifi_post_counts(int wifi, time_t ts) {
  if (!gWifiHttpQueue) return;
  http_msg_t m { wifi, ts };
  (void) xQueueSend(gWifiHttpQueue, &m, 0);
}

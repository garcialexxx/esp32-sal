#include "wifi_post.h"

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"

#include "net_time.h"
#include "sdcard.h"     // sdjson_delete_first_lines()

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <ctype.h>      // isspace
#include <stdlib.h>     // malloc/realloc/free

#include <esp_heap_caps.h>
extern "C" {
  #include "esp_system.h"
  #include "esp_bt.h"
}

/* ── PROTOTIPOS DEL LOGGER SD ───────────────────────────────────────────── */
extern bool sdjson_logger_start(void);
extern void sdjson_logger_stop(void);
extern "C" bool sdjson_delete_first_lines(size_t n);
/* Cerrar la línea actual de forma SÍNCRONA antes de postear (NO usado) */
extern "C" bool sdcard_newline_sync(uint32_t timeout_ms);
/* Cerrar la línea actual (asíncrono) */
extern "C" void sdcard_newline(void);
/* APPEND para {t,w} cuando no hay Wi-Fi */
extern "C" void sdcard_append_jsonl(const char *chunk);
/* Solicitar purga de líneas antiguas al writer (seguro y sin carreras) */
extern "C" void sdjson_request_purge_older_than(uint32_t max_age_sec);
/* ¿Hora real disponible? (NTP listo) */
extern "C" bool netTimeReady(void);
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
//#define POST_URL "https://plataforma.phebus.net:443/api/v1/Pl08nZ92k1eYZhXxj9ca/telemetry"
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

// --- Watchdog / diagnóstico de POST ---
static TaskHandle_t  gPostResetTask = nullptr;
// ticks de FreeRTOS para medir progreso real de POST
static volatile uint32_t gLastPostOkTick  = 0; // último POST OK completado (o saneado vacío considerado OK)
static volatile uint32_t gLastPostTryTick = 0; // instante en que entramos en sendRequest()

// Reboot orquestado (para evitar bloqueos antes del restart)
static volatile bool gRebootScheduled = false;

/* ── Declaración adelantada ──────────────────────────────────────────────── */
static bool post_file_and_delete_on_ok(const char* fullpath, int wifiCount, time_t ts);

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

/* Utilidades de diagnóstico del snapshot ---------------------------------- */

static char last_non_ws_char_in_file(const char* path) {
  FILE* f = fopen(path, "rb");
  if (!f) return '\0';
  if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return '\0'; }
  long pos = ftell(f);
  if (pos <= 0) { fclose(f); return '\0'; }
  int c = 0;
  for (long i = pos - 1; i >= 0; --i) {
    fseek(f, i, SEEK_SET);
    c = fgetc(f);
    if (c == EOF) break;
    if (!isspace(c)) { fclose(f); return (char)c; }
  }
  fclose(f);
  return '\0';
}

// Devuelve true si detecta alguna línea no vacía que no termina en '}' (o no empieza en '{')
static bool find_bad_jsonish_line(const char* path, size_t* out_bad_line, char* out_endchar) {
  FILE* f = fopen(path, "r");
  if (!f) return false;
  char line[1024];
  size_t lineno = 0;
  while (fgets(line, sizeof(line), f)) {
    lineno++;
    // recortar CR/LF y espacios
    int len = (int)strlen(line);
    while (len > 0 && (line[len-1] == '\n' || line[len-1] == '\r' || isspace((unsigned char)line[len-1]))) len--;
    line[len] = '\0';
    int start = 0;
    while (line[start] && isspace((unsigned char)line[start])) start++;

    if (len - start <= 0) continue; // línea vacía

    char first = line[start];
    char last  = line[len-1];

    // Heurística: esperamos objetos JSON por línea -> empiezan con '{' y acaban con '}'
    if (first != '{' || last != '}') {
      if (out_bad_line) *out_bad_line = lineno;
      if (out_endchar)  *out_endchar  = last;
      fclose(f);
      return true;
    }
  }
  fclose(f);
  return false;
}

static void diag_snapshot_shape(const char* path) {
  NdjsonStats st = {};
  (void)compute_ndjson_stats(path, st);
  char tail = last_non_ws_char_in_file(path);
  size_t bad_line = 0;
  char   bad_end  = 0;
  bool has_bad = find_bad_jsonish_line(path, &bad_line, &bad_end);

  Serial.printf("[DIAG] snapshot '%s': lines=%u bytes=%u tail=0x%02X '%c'%s",
                path, (unsigned)st.lines, (unsigned)st.bytes,
                (unsigned char)tail, (tail ? tail : ' '),
                (has_bad ? " [BAD-LINE]" : " [OK-LINES]"));
  if (has_bad) {
    Serial.printf(" bad_line=%u endchar=0x%02X '%c'\n",
                  (unsigned)bad_line, (unsigned char)bad_end, (bad_end ? bad_end : ' '));
  } else {
    Serial.printf("\n");
  }

  if (tail == ',') {
    Serial.println("[DIAG] ALERTA: el snapshot termina en coma ',' -> JSON resultante será inválido.");
  }
}

/* ── SANEADO: conservar objetos completos, eliminar “datos” incompletos ────
   Estrategia por línea:
   - Escanear carácter a carácter.
   - Detectar objetos top-level { ... } respetando strings y escapes.
   - Escribir solo los objetos completos separados por coma (SIN coma final).
   - Ignorar comas y espacios fuera de objetos; descartar el objeto si la línea
     termina con el objeto a medias (profundidad > 0).
*/

static void sb_append(char** buf, size_t* cap, size_t* len, char c) {
  if (*len + 1 >= *cap) {
    size_t ncap = (*cap == 0) ? 256 : (*cap * 2);
    char* nb = (char*)realloc(*buf, ncap);
    if (!nb) return; // sin memoria: se truncará silenciosamente
    *buf = nb; *cap = ncap;
  }
  (*buf)[(*len)++] = c;
  (*buf)[*len] = '\0';
}

static bool sanitize_snapshot_inplace(const char* path, size_t* out_kept, size_t* out_dropped) {
  if (out_kept)    *out_kept = 0;
  if (out_dropped) *out_dropped = 0;

  FILE* fi = fopen(path, "rb");
  if (!fi) return false;

  char tmpPath[128];
  snprintf(tmpPath, sizeof(tmpPath), "%s.san", path);
  FILE* fo = fopen(tmpPath, "wb");
  if (!fo) { fclose(fi); return false; }

  size_t kept_total = 0, dropped_total = 0;

  bool in_string = false;
  bool esc = false;
  int  depth = 0;            // profundidad de llaves { }
  bool have_obj = false;     // tenemos un objeto en curso (depth>0 desde que vimos '{')

  char* objbuf = nullptr;    // buffer dinámico del objeto en curso
  size_t obcap = 0, oblen = 0;

  bool wrote_any_in_line = false; // si se escribió algún objeto en la línea actual

  int ch;
  while ((ch = fgetc(fi)) != EOF) {
    if (ch == '\r') continue; // ignorar CR

    if (depth == 0) {
      // NO dentro de objeto
      if (ch == '{') {
        // empezar objeto
        in_string = false; esc = false; depth = 1; have_obj = true;
        oblen = 0; // reset buffer
        sb_append(&objbuf, &obcap, &oblen, '{');
      } else if (ch == '\n') {
        // fin de línea: si estábamos a medias de un objeto (no debería), descartar
        if (have_obj && depth > 0) {
          dropped_total++; // objeto incompleto
          have_obj = false; depth = 0; in_string = false; esc = false;
          oblen = 0;
        }
        if (wrote_any_in_line) {
          fputc('\n', fo);
          wrote_any_in_line = false;
        }
        // si no se escribió nada en la línea, simplemente la omitimos
      } else {
        // fuera de objeto: comas sueltas, espacios, basura -> ignorar
      }
    } else {
      // DENTRO de objeto: copiar char, gestionar estado
      sb_append(&objbuf, &obcap, &oblen, (char)ch);

      if (in_string) {
        if (esc) { esc = false; }
        else if (ch == '\\') { esc = true; }
        else if (ch == '"') { in_string = false; }
      } else {
        if (ch == '"') {
          in_string = true;
        } else if (ch == '{') {
          depth++;
        } else if (ch == '}') {
          depth--;
          if (depth == 0) {
            // Objeto completo: escribirlo (con coma si no es el primero de la línea)
            if (wrote_any_in_line) fputc(',', fo);
            if (oblen > 0) fwrite(objbuf, 1, oblen, fo);
            kept_total++;
            wrote_any_in_line = true;
            have_obj = false; oblen = 0;
          }
        }
        // los corchetes [] no afectan al cierre del objeto top-level
      }

      // Si llega un \n mientras depth>0, lo tratamos en la rama superior al leerlo.
    }
  }

  // EOF: si quedó objeto a medias, descartar
  if (have_obj && depth > 0) {
    dropped_total++;
    have_obj = false;
    oblen = 0;
  }
  // Cerrar última línea si escribimos algo y no había \n final
  if (wrote_any_in_line) {
    fputc('\n', fo);
  }

  free(objbuf);
  fclose(fi);
  fclose(fo);

  if (out_kept)    *out_kept = kept_total;
  if (out_dropped) *out_dropped = dropped_total;

  if (kept_total == 0) {
    // nada útil -> eliminar saneado y original
    remove(tmpPath);
    Serial.printf("[SAN] '%s': kept=0 dropped=%u -> archivo eliminado, no se postea.\n",
                  path, (unsigned)dropped_total);
    remove(path);
    return true; // saneado correcto, pero sin datos
  }

  // Reemplazar original por saneado
  remove(path);
  if (rename(tmpPath, path) != 0) {
    Serial.printf("[SAN] '%s': kept=%u dropped=%u -> ERROR al reemplazar snapshot\n",
                  path, (unsigned)kept_total, (unsigned)dropped_total);
    // best effort: dejamos el .san
    return false;
  }

  Serial.printf("[SAN] '%s': kept=%u dropped=%u -> snapshot saneado OK\n",
                path, (unsigned)kept_total, (unsigned)dropped_total);
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

    while (_buf_len < (int)sizeof(_buf)) {
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
        _in_line = true;
        _line_started = false;
        if (_need_comma_between_lines) {
          if (_buf_len < (int)sizeof(_buf)) _buf[_buf_len++] = ',';
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
        if (_buf_len < (int)sizeof(_buf)) {
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

// Espera a que el archivo 'path' termine en '}' y su tamaño permanezca
// estable durante 'settle_ms'. Devuelve true si está listo antes del timeout.
static bool wait_file_stable_closed(const char* path,
                                    uint32_t timeout_ms = 800,
                                    uint32_t settle_ms  = 120) {
  uint32_t start   = millis();
  long     last_sz = -1;
  uint32_t last_change = millis();

  for (;;) {
    // Obtener tamaño actual
    long cur_sz = -1;
    FILE* f = fopen(path, "rb");
    if (f) {
      fseek(f, 0, SEEK_END);
      cur_sz = ftell(f);
      fclose(f);
    }

    if (cur_sz != last_sz) {
      last_sz = cur_sz;
      last_change = millis();
    }

    // Comprobamos último char no-blanco
    char tail = last_non_ws_char_in_file(path);

    // Condición de “sellado”: termina en '}' y tamaño estable lo suficiente
    if (tail == '}' && (millis() - last_change) >= settle_ms && last_sz > 0) {
      return true;
    }

    if ((millis() - start) >= timeout_ms) {
      return false; // timeout
    }

    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

// Cuenta objetos JSON top-level en todo el archivo (todas las líneas).
// Asume que el archivo ya está saneado (objetos completos).
static size_t count_events_in_file(const char* path) {
  FILE* f = fopen(path, "rb");
  if (!f) return 0;

  bool in_string = false, esc = false;
  int  depth = 0;
  size_t count = 0;
  int ch;

  while ((ch = fgetc(f)) != EOF) {
    if (in_string) {
      if (esc) { esc = false; }
      else if (ch == '\\') { esc = true; }
      else if (ch == '"') { in_string = false; }
      continue;
    }

    if (ch == '"') {
      in_string = true;
      continue;
    }

    if (ch == '{') {
      depth++;
    } else if (ch == '}') {
      if (depth > 0) {
        depth--;
        if (depth == 0) count++; // cerramos un objeto top-level
      }
    }
  }

  fclose(f);
  return count;
}

static void rebooter_task(void *arg) {
  (void)arg;
  // pequeño retardo para que salgan logs por UART/USB
  for (int i=0; i<5; ++i) {
    Serial.println("[WATCHDOG] Reinicio programado...");
    vTaskDelay(pdMS_TO_TICKS(200));
  }
  esp_restart(); // no retorna
  vTaskDelete(NULL);
}

static void schedule_reboot_nonblocking(const char* reason) {
  if (gRebootScheduled) return;
  gRebootScheduled = true;

  Serial.printf("[WATCHDOG] %s\n", reason ? reason : "Reinicio solicitado");
  // Evitar operaciones bloqueantes aquí. Solo un sellado asíncrono (no bloquea).
  sdcard_newline();

  // Alta prioridad, cualquier core
  xTaskCreatePinnedToCore(rebooter_task, "rebooter", 3072, NULL, configMAX_PRIORITIES - 1, NULL, tskNO_AFFINITY);
}

/**
 * @brief Envía el contenido de un archivo específico vía POST.
 *        (Con diagnóstico y saneado previo de datos incompletos)
 *        CORREGIDO: recuento_max = nº de objetos realmente presentes.
 */
static bool post_file_and_delete_on_ok(const char* fullpath, int /*wifiCount*/, time_t ts) {
    NdjsonStats st = {};
    (void)compute_ndjson_stats(fullpath, st);

    // DIAGNÓSTICO DEL SNAPSHOT (antes de saneado)
    diag_snapshot_shape(fullpath);

    // SANEADO: conservar solo objetos completos y quitar comas/datos incompletos
    size_t kept = 0, dropped = 0;
    bool saneado_ok = sanitize_snapshot_inplace(fullpath, &kept, &dropped);
    if (!saneado_ok) {
        Serial.println("[SAN] ERROR de saneado (se intentará más tarde).");
        return false;
    }
    if (kept == 0) {
        // Consideramos que hubo “progreso” (no hay nada que enviar)
        gLastPostOkTick = xTaskGetTickCount();
        Serial.println("[HTTP] Tras saneado no queda nada -> no posteo.");
        return true; // no es error
    }

    // Recalcular stats tras saneado
    (void)compute_ndjson_stats(fullpath, st);

    if (st.lines == 0 || st.bytes == 0) {
        Serial.printf("[HTTP] Snapshot vacío ('%s'), borrando...\n", fullpath);
        remove(fullpath);
        gLastPostOkTick = xTaskGetTickCount();
        return true; // nada que enviar, no es error
    }

    // === Conteo real de eventos (objetos top-level) en el snapshot saneado ===
    size_t eventos_reales = count_events_in_file(fullpath);
    Serial.printf("[HTTP] eventos reales en snapshot: %u\n", (unsigned)eventos_reales);

    if (eventos_reales == 0) {
        // Saneado dejó algo raro (p. ej., solo espacios). No enviamos.
        remove(fullpath);
        gLastPostOkTick = xTaskGetTickCount();
        Serial.println("[HTTP] Snapshot sin objetos tras saneado -> no posteo.");
        return true;
    }

    // Construimos el JSON contenedor
    char prefix[128];
    snprintf(prefix, sizeof(prefix), "{\"recuento_max\":%u,\"ts\":%lu,\"events\":[",
             (unsigned)eventos_reales, (unsigned long)ts);
    const size_t prefix_len = strlen(prefix);

    static const char suffix[] = "]}";
    const size_t suffix_len = sizeof(suffix) - 1;

    // Comas entre líneas (insertadas por el streamer) -> (lines - 1)
    size_t commas_between_lines = (st.lines > 0) ? (st.lines - 1) : 0;

    // Longitud total que enviaremos por streaming
    size_t content_len = prefix_len + st.bytes + commas_between_lines + suffix_len;

    log_mem("antes POST");
    log_stack_watermark("antes POST");

    if (!have_tls_memory()) {
        Serial.println("[HTTP] Heap insuficiente para TLS -> se reintentará más tarde.");
        return false;
    }

    WiFiClientSecure client;
    client.setInsecure();
    client.setTimeout(8000);         // socket RX/TX timeout

    HTTPClient http;
    http.setConnectTimeout(5000);    // timeout de conexión TCP
    http.setTimeout(12000);          // timeout de lectura/cabeceras

    if (!http.begin(client, POST_URL)) {
        Serial.println("[HTTP] begin() falló");
        return false;
    }
    http.addHeader("Content-Type", "application/json");

    NdjsonArrayStream streamer(fullpath, prefix, prefix_len, suffix, suffix_len);
    streamer.begin();

    gLastPostTryTick = xTaskGetTickCount();
    int code = http.sendRequest("POST", &streamer, content_len);
    streamer.end();

    http.end();
    log_mem("despues POST");
    log_stack_watermark("despues POST");

    bool ok = (code > 0 && code < 400);
    if (ok) {
        gLastPostOkTick = xTaskGetTickCount();
        Serial.printf("[HTTP] POST OK (%d), borrando '%s'\n", code, fullpath);
        if (remove(fullpath) != 0) {
            Serial.printf("[HTTP] WARNING: no se pudo borrar el archivo '%s'\n", fullpath);
        }
    } else {
        #if defined(HTTPCLIENT_1_2_COMPATIBLE) || ARDUINO
          Serial.printf("[HTTP] POST FAIL (%d) %s\n", code, HTTPClient::errorToString(code).c_str());
        #else
          Serial.printf("[HTTP] POST FAIL (%d)\n", code);
        #endif
    }

    return ok;
}


static void wifi_http_task(void *pvParameters) {
  (void) pvParameters;

  (void)connectToWiFi_local();
  netTimeInit();

  http_msg_t m;
  
  // Nombres de archivos que usaremos
  char live_path[96];
  char sending_path[96];
  snprintf(live_path, sizeof(live_path), "%s/%s.jsonl", MOUNT_POINT, SDCARD_MACLOG_BASENAME);
  snprintf(sending_path, sizeof(sending_path), "%s/%s_sending.jsonl", MOUNT_POINT, SDCARD_MACLOG_BASENAME);

  for (;;) {
    if (gRebootScheduled) { vTaskDelay(pdMS_TO_TICKS(50)); continue; }

    if (xQueueReceive(gWifiHttpQueue, &m, portMAX_DELAY) != pdTRUE)
      continue;

    bool nowConnected = (WiFi.status() == WL_CONNECTED);
    if (!nowConnected) (void)connectToWiFi_local(3000);
    nowConnected = (WiFi.status() == WL_CONNECTED);

    // Si NO hay Wi-Fi, guardamos el recuento y sellamos la línea como antes
    if (!nowConnected) {
      if (netTimeReady()) {
        char line[64];
        snprintf(line, sizeof(line), "{\"t\":%lu,\"w\":%d}", (unsigned long)m.ts, m.wifi);
        sdcard_append_jsonl(line);
      } else {
        Serial.println("[HTTP] Sin Wi-Fi y sin hora real: NO se guarda en SD.");
      }
      sdcard_newline();
      Serial.println("[HTTP] Sin Wi-Fi: lote sellado (equivalente a intento de POST).");
      continue;
    }

    // --- DESACOPLAMIENTO (snapshot + envío) ---
    // 1) Pedimos sellar la línea actual (asíncrono)
    Serial.println("[HTTP] Sincronizando datos pendientes antes del snapshot...");
    sdcard_newline();

    // 2) Esperamos a que el archivo 'live' esté listo para snapshot
    if (!wait_file_stable_closed(live_path, /*timeout_ms*/800, /*settle_ms*/120)) {
      Serial.println("[HTTP] WARNING: snapshot no listo (archivo no estable o sin '}' final). Se pospone.");
      // No paramos el logger ni renombramos: en la siguiente ronda lo intentamos de nuevo
      continue;
    }

    Serial.println("[HTTP] Preparando backlog para envío...");
    sdjson_logger_stop(); // detenemos el writer para congelar el archivo

    // ¿Quedó un archivo pendiente de un intento anterior?
    bool pending_file_exists = false;
    FILE* f_pending = fopen(sending_path, "r");
    if (f_pending) { fclose(f_pending); pending_file_exists = true; }

    // Renombramos el archivo "en vivo" a "_sending" si no hay uno pendiente
    bool renamed_ok = false;
    FILE* f_live = fopen(live_path, "r");
    if (f_live) {
        fclose(f_live);
        if (!pending_file_exists) {
            if (rename(live_path, sending_path) == 0) {
                renamed_ok = true;
                Serial.printf("[HTTP] Archivo '%s' renombrado a '%s'\n", live_path, sending_path);
            } else {
                Serial.println("[HTTP] ERROR: Falló el renombrado del archivo de backlog.");
            }
        } else {
            Serial.println("[HTTP] Hay un archivo pendiente de envío anterior, no se renombra el actual.");
        }
    } else {
        Serial.println("[HTTP] No hay archivo de backlog 'en vivo' para enviar.");
    }
    
    // Reiniciamos el writer inmediatamente: se empieza a escribir en un nuevo mac_events.jsonl
    sdjson_logger_start();

    // Intentamos enviar el archivo renombrado (o el pendiente anterior)
    if (renamed_ok || pending_file_exists) {
        Serial.printf("[HTTP] Intentando POST del archivo '%s'...\n", sending_path);
        bool post_ok = post_file_and_delete_on_ok(sending_path, m.wifi, m.ts);
        
        if (!post_ok) {
            Serial.println("[HTTP] POST falló. El archivo se conservará para el próximo intento.");
        }
    }

    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

/* ── TAREA PERIÓDICA: purga cada 6 minutos cuando no hay Internet ───────── */

static void backlog_purge_task(void *pvParameters) {
  (void)pvParameters;
  const uint32_t kIntervalMs = 6 * 60 * 1000; // 6 minutos
  const uint32_t kMaxAgeSecs = 48 * 60 * 60;  // 48h

  for (;;) {
    vTaskDelay(pdMS_TO_TICKS(kIntervalMs));

    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[PURGE] Offline: solicitando purga de líneas > 48h...");
      // Lo hace la tarea writer, sin carreras
      sdjson_request_purge_older_than(kMaxAgeSecs);
    }
  }
}

/* ── WATCHDOG: reiniciar si hay Wi-Fi pero 10 min sin progreso de POST ──── */

static void post_reset_watchdog_task(void *pvParameters) {
  (void)pvParameters;
  const uint32_t kCheckPeriodMs = 60000;         // comprobación cada 1 min
  const uint32_t kNoPostLimitMs = 10 * 60 * 1000; // 10 minutos

  for (;;) {
    vTaskDelay(pdMS_TO_TICKS(kCheckPeriodMs));

    if (gRebootScheduled) continue;

    const wl_status_t st = WiFi.status();
    if (st != WL_CONNECTED) {
      continue;
    }

    // Diagnóstico simplificado
    UBaseType_t qdepth = gWifiHttpQueue ? uxQueueMessagesWaiting(gWifiHttpQueue) : 0;

    uint32_t nowTick  = xTaskGetTickCount();
    uint32_t lastTry  = gLastPostTryTick;
    uint32_t lastOk   = gLastPostOkTick;
    uint32_t sinceTryMs = (lastTry > 0 && nowTick > lastTry) ? (nowTick - lastTry) * portTICK_PERIOD_MS : 0;
    uint32_t sinceOkMs  = (lastOk > 0 && nowTick > lastOk)  ? (nowTick - lastOk)  * portTICK_PERIOD_MS : 0;

    Serial.printf("[WATCHDOG] diag: qdepth=%u, sinceTry=%ums, sinceOk=%ums\n",
                  (unsigned)qdepth, (unsigned)sinceTryMs, (unsigned)sinceOkMs);

    uint32_t lastActivityTick = (lastOk > lastTry) ? lastOk : lastTry;
    if (lastActivityTick == 0) continue;

    uint32_t elapsedMs = (nowTick - lastActivityTick) * portTICK_PERIOD_MS;
    if (nowTick < lastActivityTick) { // wrap de ticks
        elapsedMs = ((0xFFFFFFFFu - lastActivityTick) + nowTick) * portTICK_PERIOD_MS;
    }

    if (elapsedMs >= kNoPostLimitMs) {
      if (lastTry > lastOk) {
        schedule_reboot_nonblocking("Posible cuelgue en sendRequest(): 10 min sin finalizar intento");
      } else {
        // reinicia solo si hay algo pendiente
        char sending_path[96];
        snprintf(sending_path, sizeof(sending_path), "%s/%s_sending.jsonl", MOUNT_POINT, SDCARD_MACLOG_BASENAME);
        FILE* f = fopen(sending_path, "r");
        bool has_pending_file = (f != NULL);
        if (f) fclose(f);

        if (qdepth > 0 || has_pending_file) {
            schedule_reboot_nonblocking("10 min sin POST OK con Wi-Fi y datos pendientes");
        } else {
            Serial.println("[WATCHDOG] 10 min sin POST OK y sin datos pendientes. No se reinicia.");
        }
      }
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
    xTaskCreatePinnedToCore(
      wifi_http_task, "wifi_http_task", 12288, NULL, 1, &gWifiHttpTask, 1
    );
  }
  if (!gPurgeTask) {
    xTaskCreatePinnedToCore(
      backlog_purge_task, "sd_purge_task", 4096, NULL, 1, &gPurgeTask, 1
    );
  }
  if (!gPostResetTask) {
    uint32_t nowTick = xTaskGetTickCount();
    gLastPostTryTick = nowTick;
    gLastPostOkTick  = nowTick;

    xTaskCreatePinnedToCore(
      post_reset_watchdog_task, "post_reset_wd", 4096, NULL, 1, &gPostResetTask, 1
    );
  }
}

void wifi_post_counts(int wifi, time_t ts) {
  if (!gWifiHttpQueue) return;
  http_msg_t m { wifi, ts };
  if (xQueueSend(gWifiHttpQueue, &m, 0) != pdTRUE) {
    Serial.println("[HTTP] queue full -> dropped");
  }
}

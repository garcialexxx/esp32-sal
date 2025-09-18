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
/* APPEND de objetos crudos NDJSON */
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
//#define POST_URL "https://plataforma.phebus.net:443/api/v1/YQARkOKOcKThFSGIAWar/telemetry"
#define POST_URL "https://plataforma.phebus.net:443/api/v1/Pl08nZ92k1eYZhXxj9ca/telemetry"
//#define POST_URL "https://plataforma.phebus.net:443/api/v1/4b4Fm1WfHCIEf8kFQPxU/telemetry"
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
#define STREAM_CHUNK_MAX  500
#endif

#ifndef MOUNT_POINT
#define MOUNT_POINT "/sdcard"
#endif
#ifndef SDCARD_MACLOG_BASENAME
#define SDCARD_MACLOG_BASENAME "mac_events"
#endif

// ── NUEVO: vaciado por offset (rápido)
#ifndef MAX_LINES_PER_POST
#define MAX_LINES_PER_POST 25      // ajustable
#endif
#define SENDING_PATH  MOUNT_POINT "/" SDCARD_MACLOG_BASENAME "_sending.jsonl"
#define INDEX_PATH    MOUNT_POINT "/" SDCARD_MACLOG_BASENAME "_sending.idx"
#define CHUNK_PATH    MOUNT_POINT "/" SDCARD_MACLOG_BASENAME "_chunk.jsonl"
#define COMPACT_MIN_BYTES (256UL * 1024UL)      // compactar si cursor > 256KB
#define COMPACT_FRAC_NUM    1                   // compactar si cursor > 1/2 del archivo
#define COMPACT_FRAC_DEN    2

typedef struct {
  int    wifi;
  time_t ts;
} http_msg_t;

static QueueHandle_t gWifiHttpQueue = nullptr;
static TaskHandle_t  gWifiHttpTask  = nullptr;
static TaskHandle_t  gPurgeTask     = nullptr;

// --- Watchdog / diagnóstico de POST ---
static TaskHandle_t  gPostResetTask = nullptr;
static volatile uint32_t gLastPostOkTick  = 0;
static volatile uint32_t gLastPostTryTick = 0;

static volatile bool gRebootScheduled = false;

/* ── Declaración adelantada ──────────────────────────────────────────────── */
struct NdjsonStats { size_t lines=0; size_t bytes=0; };
static bool compute_ndjson_stats(const char* path, NdjsonStats& st);
static char last_non_ws_char_in_file(const char* path);
static bool sanitize_snapshot_inplace(const char* path, size_t* out_kept, size_t* out_dropped);
class NdjsonArrayStream;
static bool wait_file_stable_closed(const char* path, uint32_t timeout_ms , uint32_t settle_ms );

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

/* ── Stats y diagnóstico básicos ─────────────────────────────────────────── */

static bool compute_ndjson_stats(const char* path, NdjsonStats& st) {
  FILE* f = fopen(path, "r");
  if (!f) { st.lines = st.bytes = 0; return false; }
  size_t lines = 0, bytes = 0, cur = 0; int c;
  while ((c = fgetc(f)) != EOF) {
    if (c == '\r') continue;
    if (c == '\n') { if (cur > 0) { lines++; bytes += cur; cur = 0; } }
    else { cur++; }
  }
  if (cur > 0) { lines++; bytes += cur; }
  fclose(f);
  st.lines = lines; st.bytes = bytes; return true;
}

static char last_non_ws_char_in_file(const char* path) {
  FILE* f = fopen(path, "rb"); if (!f) return '\0';
  if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return '\0'; }
  long pos = ftell(f);
  if (pos <= 0) { fclose(f); return '\0'; }
  int c = 0;
  for (long i = pos - 1; i >= 0; --i) {
    fseek(f, i, SEEK_SET); c = fgetc(f);
    if (c == EOF) break;
    if (!isspace(c)) { fclose(f); return (char)c; }
  }
  fclose(f); return '\0';
}

/* ── Saneado (conserva objetos completos) ────────────────────────────────── */

static void sb_append(char** buf, size_t* cap, size_t* len, char c) {
  if (*len + 1 >= *cap) {
    size_t ncap = (*cap == 0) ? 256 : (*cap * 2);
    char* nb = (char*)realloc(*buf, ncap);
    if (!nb) return;
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

  char tmpPath[128]; snprintf(tmpPath, sizeof(tmpPath), "%s.san", path);
  FILE* fo = fopen(tmpPath, "wb"); if (!fo) { fclose(fi); return false; }

  size_t kept_total = 0, dropped_total = 0;
  bool in_string=false, esc=false, have_obj=false; int depth=0;
  char* objbuf=nullptr; size_t obcap=0, oblen=0; bool wrote_any_in_line=false;

  int ch;
  while ((ch = fgetc(fi)) != EOF) {
    if (ch == '\r') continue;
    if (depth == 0) {
      if (ch == '{') { in_string=false; esc=false; depth=1; have_obj=true; oblen=0; sb_append(&objbuf,&obcap,&oblen,'{'); }
      else if (ch == '\n') {
        if (have_obj && depth > 0) { dropped_total++; have_obj=false; depth=0; in_string=false; esc=false; oblen=0; }
        if (wrote_any_in_line) { fputc('\n', fo); wrote_any_in_line=false; }
      } else { /* basura fuera de objeto: ignorar */ }
    } else {
      sb_append(&objbuf,&obcap,&oblen,(char)ch);
      if (in_string) {
        if (esc) esc=false;
        else if (ch=='\\') esc=true;
        else if (ch=='"') in_string=false;
      } else {
        if (ch=='"') in_string=true;
        else if (ch=='{') depth++;
        else if (ch=='}') {
          if (--depth == 0) {
            if (wrote_any_in_line) fputc(',', fo);
            if (oblen>0) fwrite(objbuf,1,oblen,fo);
            kept_total++; wrote_any_in_line=true; have_obj=false; oblen=0;
          }
        }
      }
    }
  }
  if (have_obj && depth>0) { dropped_total++; have_obj=false; oblen=0; }
  if (wrote_any_in_line) fputc('\n', fo);

  free(objbuf); fclose(fi); fclose(fo);

  if (kept_total == 0) {
    remove(tmpPath); remove(path);
    Serial.printf("[SAN] '%s': kept=0 dropped=%u -> nada útil\n", path, (unsigned)dropped_total);
    return true;
  }
  remove(path);
  if (rename(tmpPath, path) != 0) {
    Serial.printf("[SAN] '%s': kept=%u dropped=%u -> ERROR al reemplazar\n",
                  path, (unsigned)kept_total, (unsigned)dropped_total);
    return false;
  }
  Serial.printf("[SAN] '%s': kept=%u dropped=%u -> OK\n",
                path, (unsigned)kept_total, (unsigned)dropped_total);
  return true;
}

/* ── Stream para POST del NDJSON envuelto en JSON ───────────────────────── */

class NdjsonArrayStream : public Stream {
public:
  NdjsonArrayStream(const char* path,
                    const char* prefix, size_t prefix_len,
                    const char* suffix, size_t suffix_len)
  : _f(nullptr), _path(path),
    _prefix(prefix), _prefix_len(prefix_len), _prefix_pos(0),
    _suffix(suffix), _suffix_len(suffix_len), _suffix_pos(0),
    _state(STATE_PREFIX),
    _need_comma_between_lines(false), _in_line(false), _line_started(false),
    _buf_len(0), _buf_pos(0) {}

  bool begin() {
    _f = fopen(_path, "r");
    _buf_len = _buf_pos = 0;
    _need_comma_between_lines = false;
    _in_line = false; _line_started = false;
    _state = STATE_PREFIX;
    return true;
  }
  void end() { if (_f) { fclose(_f); _f=nullptr; } }

  int available() override {
    if (_buf_pos < _buf_len) return (int)(_buf_len - _buf_pos);

    if (_state == STATE_PREFIX) {
      if (_prefix_pos < _prefix_len) {
        size_t remain = _prefix_len - _prefix_pos;
        size_t n = (remain > STREAM_CHUNK_MAX) ? STREAM_CHUNK_MAX : remain;
        memcpy(_buf, _prefix + _prefix_pos, n);
        _prefix_pos += n; _buf_len = n; _buf_pos = 0; return (int)n;
      } else { _state = STATE_FILE; }
    }

    if (_state == STATE_FILE) {
      int n = fill_from_file(); if (n > 0) return n;
      _state = STATE_SUFFIX;
    }

    if (_state == STATE_SUFFIX) {
      if (_suffix_pos < _suffix_len) {
        size_t remain = _suffix_len - _suffix_pos;
        size_t n = (remain > STREAM_CHUNK_MAX) ? STREAM_CHUNK_MAX : remain;
        memcpy(_buf, _suffix + _suffix_pos, n);
        _suffix_pos += n; _buf_len = n; _buf_pos = 0; return (int)n;
      } else { _state = STATE_DONE; }
    }
    return 0;
  }

  int read() override { if (available() <= 0) return -1; return _buf[_buf_pos++]; }
  int peek() override { if (available() <= 0) return -1; return _buf[_buf_pos]; }
  void flush() override {}
  size_t write(uint8_t) override { return 0; }

private:
  enum State { STATE_PREFIX, STATE_FILE, STATE_SUFFIX, STATE_DONE };

  int fill_from_file() {
    _buf_len = _buf_pos = 0;
    if (!_f) return 0;
    while (_buf_len < (int)sizeof(_buf)) {
      int c = fgetc(_f);
      if (c == EOF) {
        if (_in_line && _line_started) { _in_line = false; _need_comma_between_lines = true; }
        break;
      }
      if (c == '\r') continue;

      if (!_in_line) {
        if (c == '\n') continue;
        _in_line = true; _line_started = false;
        if (_need_comma_between_lines) {
          if (_buf_len < (int)sizeof(_buf)) _buf[_buf_len++] = ',';
          else { ungetc(c,_f); break; }
          _need_comma_between_lines = false;
        }
      }

      if (c == '\n') {
        _in_line = false; _need_comma_between_lines = true;
        if (_buf_len == 0) continue; else break;
      } else {
        if (_buf_len < (int)sizeof(_buf)) { _buf[_buf_len++] = (uint8_t)c; _line_started = true; }
        else break;
      }
    }
    return (int)_buf_len;
  }

  FILE*  _f;
  const char* _path;
  const char* _prefix; size_t _prefix_len; size_t _prefix_pos;
  const char* _suffix; size_t _suffix_len; size_t _suffix_pos;
  State  _state;
  bool   _need_comma_between_lines, _in_line, _line_started;
  uint8_t _buf[STREAM_CHUNK_MAX]; size_t _buf_len, _buf_pos;
};

/* ── Utilidades de envío ─────────────────────────────────────────────────── */

static size_t count_events_in_file(const char* path) {
  FILE* f = fopen(path, "rb"); if (!f) return 0;
  bool in_string=false, esc=false; int depth=0; size_t count=0; int ch;
  while ((ch=fgetc(f))!=EOF) {
    if (in_string) { if (esc) esc=false; else if (ch=='\\') esc=true; else if (ch=='"') in_string=false; continue; }
    if (ch=='{') depth++;
    else if (ch=='}') { if (depth>0 && --depth==0) count++; }
  }
  fclose(f); return count;
}

static void rebooter_task(void *arg) {
  (void)arg;
  for (int i=0;i<5;++i) { Serial.println("[WATCHDOG] Reinicio programado..."); vTaskDelay(pdMS_TO_TICKS(200)); }
  esp_restart();
}

static void schedule_reboot_nonblocking(const char* reason) {
  if (gRebootScheduled) return;
  gRebootScheduled = true;
  Serial.printf("[WATCHDOG] %s\n", reason ? reason : "Reinicio solicitado");
  sdcard_newline();
  xTaskCreatePinnedToCore(rebooter_task, "rebooter", 3072, NULL, configMAX_PRIORITIES-1, NULL, tskNO_AFFINITY);
}

/* ── NUEVO: manejo por cursor/offset ─────────────────────────────────────── */

// Carga/guarda cursor (offset en bytes) del archivo SENDING_PATH.
static bool load_cursor(size_t& off) {
  FILE* f = fopen(INDEX_PATH, "r");
  if (!f) { off = 0; return true; }
  unsigned long v=0; int r = fscanf(f, "%lu", &v); fclose(f);
  if (r==1) { off = (size_t)v; return true; }
  off = 0; return false;
}
static bool save_cursor(size_t off) {
  FILE* f = fopen(INDEX_PATH, "w");
  if (!f) return false;
  fprintf(f, "%lu\n", (unsigned long)off);
  fclose(f); return true;
}
static void reset_cursor() {
  remove(INDEX_PATH);
  save_cursor(0);
}

// Crea chunk desde 'start_offset' leyendo como máx. 'max_lines'.
// Devuelve líneas y bytes leídos de SENDING_PATH.
static bool make_chunk_from_offset(const char* src, const char* dst,
                                   size_t max_lines,
                                   size_t start_offset,
                                   size_t* out_lines,
                                   size_t* out_bytes)
{
  if (out_lines) *out_lines = 0;
  if (out_bytes) *out_bytes = 0;

  FILE* fi = fopen(src, "rb");
  if (!fi) return false;

  fseek(fi, 0, SEEK_END);
  long total = ftell(fi);
  if ((long)start_offset >= total) { fclose(fi); return false; }

  fseek(fi, (long)start_offset, SEEK_SET);
  FILE* fo = fopen(dst, "wb"); if (!fo) { fclose(fi); return false; }

  size_t lines=0, bytes=0; int ch;
  while (lines < max_lines && (ch=fgetc(fi)) != EOF) {
    if (ch == '\r') continue;
    fputc(ch, fo); bytes++;
    if (ch == '\n') lines++;
  }
  fclose(fo); fclose(fi);

  if (out_lines) *out_lines = lines;
  if (out_bytes) *out_bytes = bytes;

  if (lines == 0) { remove(dst); return false; }
  return true;
}

// Compacta el archivo desde 'offset': copia el resto a un tmp y renombra.
static bool compact_file_from_offset(const char* path, size_t offset) {
  FILE* fi = fopen(path, "rb"); if (!fi) return false;
  if (fseek(fi, (long)offset, SEEK_SET) != 0) { fclose(fi); return false; }

  char tmp[128]; snprintf(tmp, sizeof(tmp), "%s.comp", path);
  FILE* fo = fopen(tmp, "wb"); if (!fo) { fclose(fi); return false; }

  int ch; while ((ch=fgetc(fi)) != EOF) fputc(ch, fo);
  fclose(fi); fclose(fo);

  remove(path);
  if (rename(tmp, path) != 0) { remove(tmp); return false; }
  return true;
}

// POST de un chunk NDJSON envuelto
static int post_chunk(const char* chunk_path, int wifiCount, time_t ts) {
  NdjsonStats st = {}; (void)compute_ndjson_stats(chunk_path, st);
  if (st.lines == 0 || st.bytes == 0) return 204;

  char prefix[128];
  snprintf(prefix, sizeof(prefix),
           "{\"recuento_max\":%d,\"ts\":%lu,\"events\":[",
           wifiCount, (unsigned long)ts);
  const size_t prefix_len = strlen(prefix);
  static const char suffix[] = "]}";
  const size_t suffix_len = sizeof(suffix) - 1;

  size_t commas_between_lines = (st.lines > 0) ? (st.lines - 1) : 0;
  size_t content_len = prefix_len + st.bytes + commas_between_lines + suffix_len;

  if (!have_tls_memory()) { Serial.println("[HTTP] Heap insuficiente TLS (chunk)"); return -1; }

  WiFiClientSecure client; client.setInsecure(); client.setTimeout(8000);
  HTTPClient http; http.setConnectTimeout(5000); http.setTimeout(12000);

  if (!http.begin(client, POST_URL)) { Serial.println("[HTTP] begin() falló (chunk)"); return -2; }
  http.addHeader("Content-Type", "application/json");

  NdjsonArrayStream streamer(chunk_path, prefix, prefix_len, suffix, suffix_len);
  streamer.begin();
  gLastPostTryTick = xTaskGetTickCount();
  int code = http.sendRequest("POST", &streamer, content_len);
  streamer.end(); http.end();

  if (code > 0 && code < 400) {
    gLastPostOkTick = xTaskGetTickCount();
    Serial.printf("[HTTP] POST chunk OK (%d)\n", code);
  } else {
    #if defined(HTTPCLIENT_1_2_COMPATIBLE) || ARDUINO
      Serial.printf("[HTTP] POST chunk FAIL (%d) %s\n", code, HTTPClient::errorToString(code).c_str());
    #else
      Serial.printf("[HTTP] POST chunk FAIL (%d)\n", code);
    #endif
  }
  return code;
}

/* ── (Legacy) enviar archivo entero (queda sin usar) ─────────────────────── */
static bool post_file_and_delete_on_ok(const char* fullpath, int wifiCount, time_t ts) {
  NdjsonStats st = {}; (void)compute_ndjson_stats(fullpath, st);
  size_t kept=0, dropped=0;
  bool saneado_ok = sanitize_snapshot_inplace(fullpath,&kept,&dropped);
  if (!saneado_ok) return false;
  if (kept==0) { gLastPostOkTick = xTaskGetTickCount(); return true; }

  (void)compute_ndjson_stats(fullpath, st);
  if (st.lines==0 || st.bytes==0) { remove(fullpath); gLastPostOkTick = xTaskGetTickCount(); return true; }

  char prefix[128];
  snprintf(prefix, sizeof(prefix), "{\"recuento_max\":%d,\"ts\":%lu,\"events\":[", wifiCount, (unsigned long)ts);
  const size_t prefix_len = strlen(prefix);
  static const char suffix[] = "]}";
  const size_t suffix_len = sizeof(suffix) - 1;

  size_t commas_between_lines = (st.lines>0) ? (st.lines-1) : 0;
  size_t content_len = prefix_len + st.bytes + commas_between_lines + suffix_len;

  if (!have_tls_memory()) return false;

  WiFiClientSecure client; client.setInsecure(); client.setTimeout(8000);
  HTTPClient http; http.setConnectTimeout(5000); http.setTimeout(12000);
  if (!http.begin(client, POST_URL)) return false;
  http.addHeader("Content-Type", "application/json");

  NdjsonArrayStream streamer(fullpath, prefix, prefix_len, suffix, suffix_len);
  streamer.begin();
  gLastPostTryTick = xTaskGetTickCount();
  int code = http.sendRequest("POST", &streamer, content_len);
  streamer.end(); http.end();

  bool ok = (code>0 && code<400);
  if (ok) { gLastPostOkTick = xTaskGetTickCount(); remove(fullpath); }
  return ok;
}

/* ── Task principal ──────────────────────────────────────────────────────── */

static void wifi_http_task(void *pvParameters) {
    (void) pvParameters;

    (void)connectToWiFi_local();
    netTimeInit();

    http_msg_t m;

    char live_path[96];
    snprintf(live_path, sizeof(live_path), "%s/%s.jsonl", MOUNT_POINT, SDCARD_MACLOG_BASENAME);

    for (;;) {
        if (gRebootScheduled) { vTaskDelay(pdMS_TO_TICKS(50)); continue; }

        if (xQueueReceive(gWifiHttpQueue, &m, portMAX_DELAY) != pdTRUE) continue;

        // 1. LÓGICA UNIFICADA: Crear y sellar el lote SIEMPRE
        if (netTimeReady()) {
            char line[64];
            snprintf(line, sizeof(line), "{\"t\":%lu,\"w\":%d}", (unsigned long)m.ts, m.wifi);
            sdcard_append_jsonl(line);
        } else {
            Serial.println("[HTTP] Sin hora real: NO se guarda {t,w}.");
        }
        sdcard_newline(); // Sellamos la línea actual para definir el lote.
        Serial.printf("[HTTP] Lote sellado en SD con ts=%lu\n", (unsigned long)m.ts);

        // 2. PARTE CONDICIONAL: Si no hay Wi-Fi, el trabajo de este ciclo termina aquí.
        if (WiFi.status() != WL_CONNECTED) {
            Serial.println("[HTTP] Sin Wi-Fi, el lote queda pendiente.");
            continue;
        }

        // --- HAY WI-FI: PROCEDEMOS A ENVIAR EL BACKLOG ---
        
        vTaskDelay(pdMS_TO_TICKS(150)); // Pequeño respiro para que el writer de la SD actúe

        // 3. CREAR SNAPSHOT
        // Congelar y mover live->sending si no hay un envío pendiente
        sdjson_logger_stop();

        bool pending_exists = false;
        { FILE* f = fopen(SENDING_PATH, "r"); if (f) { fclose(f); pending_exists = true; } }

        bool renamed_ok = false;
        {
            FILE* f_live = fopen(live_path, "r");
            if (f_live) {
                fclose(f_live);
                if (!pending_exists) {
                    if (rename(live_path, SENDING_PATH) == 0) {
                        renamed_ok = true;
                        reset_cursor(); // Empezamos a enviar el nuevo snapshot desde el principio
                        Serial.printf("[HTTP] Snapshot creado: '%s' -> '%s'\n", live_path, SENDING_PATH);
                    } else {
                        Serial.println("[HTTP] ERROR: Falló el renombrado del backlog.");
                    }
                } else {
                    Serial.println("[HTTP] Hay un archivo de cola pendiente; se prioriza ese.");
                }
            } else {
                Serial.println("[HTTP] No hay backlog 'en vivo' para enviar.");
            }
        }
        sdjson_logger_start();

        // 4. VACIAR SNAPSHOT POR CHUNKS
        if (renamed_ok || pending_exists) {
            size_t cursor = 0; 
            load_cursor(cursor);

            for (;;) {
                if (WiFi.status() != WL_CONNECTED) break;

                FILE* fsz = fopen(SENDING_PATH, "rb");
                if (!fsz) { remove(INDEX_PATH); break; }
                fseek(fsz, 0, SEEK_END);
                long filesize = ftell(fsz);
                fclose(fsz);

                if ((long)cursor >= filesize) {
                    remove(SENDING_PATH); remove(INDEX_PATH);
                    Serial.println("[HTTP] Cola de envío vaciada con éxito.");
                    break;
                }

                size_t lines_read = 0, bytes_read = 0;
                if (!make_chunk_from_offset(SENDING_PATH, CHUNK_PATH, MAX_LINES_PER_POST, cursor, &lines_read, &bytes_read)) {
                    if ((long)cursor >= filesize) {
                        remove(SENDING_PATH); remove(INDEX_PATH);
                        Serial.println("[HTTP] Cola vacía (alcanzado EOF).");
                    }
                    break;
                }

                // El saneado del chunk es opcional pero recomendable
                size_t kept = 0, dropped = 0;
                sanitize_snapshot_inplace(CHUNK_PATH, &kept, &dropped);

                NdjsonStats cst = {};
                compute_ndjson_stats(CHUNK_PATH, cst);
                if (cst.lines == 0 || cst.bytes == 0) {
                    cursor += bytes_read; save_cursor(cursor);
                    remove(CHUNK_PATH);
                    continue;
                }
                
                post_chunk(CHUNK_PATH, m.wifi, m.ts);
                remove(CHUNK_PATH);

                cursor += bytes_read;
                save_cursor(cursor);

                // Compactación ocasional
                if (cursor > COMPACT_MIN_BYTES && (cursor * COMPACT_FRAC_DEN) > ((size_t)filesize * COMPACT_FRAC_NUM)) {
                    Serial.printf("[HTTP] Compactando cola: cursor=%lu filesize=%ld\n", (unsigned long)cursor, filesize);
                    if (compact_file_from_offset(SENDING_PATH, cursor)) {
                        cursor = 0; save_cursor(cursor);
                    } else {
                        Serial.println("[HTTP] WARNING: compactación fallida.");
                    }
                }

                vTaskDelay(pdMS_TO_TICKS(10)); // ceder CPU
            }
        }
    }
}


/* ── Purga cada 6 minutos si no hay Internet ─────────────────────────────── */

static void backlog_purge_task(void *pvParameters) {
  (void)pvParameters;
  const uint32_t kIntervalMs = 6 * 60 * 1000;
  const uint32_t kMaxAgeSecs = 48 * 60 * 60;
  for (;;) {
    vTaskDelay(pdMS_TO_TICKS(kIntervalMs));
    if (WiFi.status() != WL_CONNECTED) {
      Serial.println("[PURGE] Offline: solicitando purga de líneas > 48h...");
      sdjson_request_purge_older_than(kMaxAgeSecs);
    }
  }
}

/* ── Watchdog de POST ───────────────────────────────────────────────────── */

static void post_reset_watchdog_task(void *pvParameters) {
  (void)pvParameters;
  const uint32_t kCheckPeriodMs = 60000;
  const uint32_t kNoPostLimitMs = 10 * 60 * 1000;

  for (;;) {
    vTaskDelay(pdMS_TO_TICKS(kCheckPeriodMs));
    if (gRebootScheduled) continue;
    if (WiFi.status() != WL_CONNECTED) continue;

    UBaseType_t qdepth = gWifiHttpQueue ? uxQueueMessagesWaiting(gWifiHttpQueue) : 0;
    uint32_t nowTick  = xTaskGetTickCount();
    uint32_t lastTry  = gLastPostTryTick;
    uint32_t lastOk   = gLastPostOkTick;
    uint32_t sinceTryMs = (lastTry>0 && nowTick>lastTry) ? (nowTick-lastTry)*portTICK_PERIOD_MS : 0;
    uint32_t sinceOkMs  = (lastOk>0 && nowTick>lastOk)   ? (nowTick-lastOk)*portTICK_PERIOD_MS  : 0;

    Serial.printf("[WATCHDOG] diag: qdepth=%u, sinceTry=%ums, sinceOk=%ums\n",
                  (unsigned)qdepth, (unsigned)sinceTryMs, (unsigned)sinceOkMs);

    uint32_t lastActivityTick = (lastOk > lastTry) ? lastOk : lastTry;
    if (lastActivityTick == 0) continue;

    uint32_t elapsedMs = (nowTick - lastActivityTick) * portTICK_PERIOD_MS;
    if (nowTick < lastActivityTick) elapsedMs = ((0xFFFFFFFFu - lastActivityTick) + nowTick) * portTICK_PERIOD_MS;

    if (elapsedMs >= kNoPostLimitMs) {
      // Si hay backlog o intentos colgados, reiniciamos
      FILE* f = fopen(SENDING_PATH, "r");
      bool has_pending_file = (f != NULL);
      if (f) fclose(f);
      if (lastTry > lastOk || qdepth > 0 || has_pending_file) {
        schedule_reboot_nonblocking("10 min sin POST OK con Wi-Fi y datos pendientes");
      }
    }
  }
}

/* ── Init / API ─────────────────────────────────────────────────────────── */

void wifi_post_init(void) {
    esp_bt_controller_mem_release(ESP_BT_MODE_BTDM);

    if (!gWifiHttpQueue)
        gWifiHttpQueue = xQueueCreate(20, sizeof(http_msg_t)); // Aumentado de 8 a 20

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
// Espera a que el archivo termine en '}' y su tamaño se mantenga estable unos ms.
static bool wait_file_stable_closed(const char* path,
                                    uint32_t timeout_ms = 800,
                                    uint32_t settle_ms  = 120) {
  uint32_t start = millis();
  long last_sz = -1;
  uint32_t last_change = millis();

  for (;;) {
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

    char tail = last_non_ws_char_in_file(path);
    if (tail == '}' && last_sz > 0 && (millis() - last_change) >= settle_ms) {
      return true; // listo para snapshot
    }

    if ((millis() - start) >= timeout_ms) {
      return false; // timeout
    }

    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

void wifi_post_counts(int wifi, time_t ts) {
  if (!gWifiHttpQueue) return;
  http_msg_t m { wifi, ts };
  if (xQueueSend(gWifiHttpQueue, &m, 0) != pdTRUE) {
    Serial.println("[HTTP] queue full -> dropped");
  }
}

#ifdef HAS_SDCARD

#include "sdcard.h"

#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"
#include "freertos/portmacro.h"   // xPortInIsrContext()

#include <Arduino.h>         // String
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <sys/unistd.h>
#include <sys/stat.h>

#ifndef TAG
#define TAG "sdcard"
#endif

sdmmc_card_t *card;

const char mount_point[] = MOUNT_POINT;
static bool useSDCard = false;

// Archivo CSV clásico
static FILE *data_file = nullptr;

#if (SDLOGGING)
static FILE *log_file = nullptr;
static FILE *uart_stdout = stdout;

int print_to_sd_card(const char *fmt, va_list args) {
  static bool static_fatal_error = false;
  static const uint32_t WRITE_CACHE_CYCLE = 5;
  static uint32_t counter_write = 0;
  int iresult;

  if (log_file == NULL) {
    printf("%s() ABORT. file handle log_file is NULL\n", __FUNCTION__);
    return -1;
  }
  if (!static_fatal_error) {
    iresult = vfprintf(log_file, fmt, args);
    if (iresult < 0) {
      printf("%s() ABORT. failed vfprintf() -> logging disabled \n", __FUNCTION__);
      static_fatal_error = true;
      return iresult;
    }
    if (counter_write++ % WRITE_CACHE_CYCLE == 0)
      fsync(fileno(log_file));
  }
  return vprintf(fmt, args);
}
#endif

static bool openFile(FILE **fd, const char *filename) {
  char _filename[128];
  snprintf(_filename, sizeof(_filename), "%s%s", MOUNT_POINT, filename);
  *fd = fopen(_filename, "a");
  if (*fd == NULL) {
    ESP_LOGE(TAG, "file <%s> open error", _filename);
    return false;
  } else {
    ESP_LOGI(TAG, "file <%s> opened", _filename);
    return true;
  }
}

/*========================
 *  LOGGER NDJSON con cola
 *========================*/

#ifndef SDCARD_MACLOG_BASENAME
#define SDCARD_MACLOG_BASENAME "mac_events"   // /sdcard/mac_events.jsonl
#endif
#ifndef SDJSON_QUEUE_LEN
#define SDJSON_QUEUE_LEN 64
#endif
#ifndef SDJSON_FLUSH_EVERY
#define SDJSON_FLUSH_EVERY 10
#endif
#ifndef SDJSON_RAW_MAXLEN
// Deja holgura para un objeto grande
#define SDJSON_RAW_MAXLEN  128
#endif

// Operaciones para el writer: appends sin salto, cierre de línea, ping/ack, purga
typedef enum {
  LOG_OP_APPEND = 0,
  LOG_OP_NEWLINE = 1,
  LOG_OP_PING = 2,
  LOG_OP_PURGE = 3   // NUEVO: purgar primeras líneas con edad > X
} logop_t;

typedef struct {
  logop_t op;
  char    raw[SDJSON_RAW_MAXLEN]; // APPEND: payload; PURGE: "max_age_sec"
} logrec_t;

static QueueHandle_t s_log_queue   = NULL;
static TaskHandle_t  s_log_task    = NULL;
static FILE*         maclog_file   = NULL;

// Estado interno: ¿ya se escribió algo en la línea actual?
static bool s_line_has_items = false;
// ACK para la versión síncrona del salto de línea (compartido también con PING)
static volatile uint32_t s_newline_ack_counter = 0;

static bool open_maclog_file(void) {
  if (maclog_file) return true;
  char path[64];
  snprintf(path, sizeof(path), "/%s.jsonl", SDCARD_MACLOG_BASENAME);
  if (!openFile(&maclog_file, path)) {
    ESP_LOGE(TAG, "sdjson: can't open %s", path);
    return false;
  }
  return true;
}

/* ========= Helpers de PURGA (ejecutan dentro de la writer) ========== */

// Leer primera línea y extraer el campo "t" (epoch). Devuelve true si OK.
static bool get_first_line_ts(time_t &out_ts) {
  char fullpath[96];
  snprintf(fullpath, sizeof(fullpath), "%s/%s.jsonl", mount_point, SDCARD_MACLOG_BASENAME);
  FILE* f = fopen(fullpath, "r");
  if (!f) return false;

  char buf[256];
  size_t i = 0;
  int c;
  bool any = false;
  while ((c = fgetc(f)) != EOF && c != '\n') {
    if (c == '\r') continue;
    if (i < sizeof(buf) - 1) buf[i++] = (char)c;
    any = true;
  }
  fclose(f);
  if (!any) return false;
  buf[i] = '\0';

  // Busca "t":<numero>
  const char *p = strstr(buf, "\"t\"");
  if (!p) p = strstr(buf, "t");
  if (!p) return false;
  p = strchr(p, ':');
  if (!p) return false;
  p++;
  while (*p == ' ' || *p == '\t') p++;

  unsigned long v = 0;
  bool has = false;
  while (*p >= '0' && *p <= '9') {
    has = true;
    v = (v * 10ul) + (unsigned)(*p - '0');
    p++;
  }
  if (!has) return false;
  out_ts = (time_t)v;
  return true;
}

// Borrar N primeras líneas (versión sin parar cola; la writer cierra/reabre FILE).
static bool sdjson_delete_first_lines_nolock(size_t n) {
  if (n == 0) return true;

  char fullpath[96];
  snprintf(fullpath, sizeof(fullpath), "%s/%s.jsonl", mount_point, SDCARD_MACLOG_BASENAME);
  char tmppath[96];
  snprintf(tmppath,  sizeof(tmppath),  "%s/%s.tmp",   mount_point, SDCARD_MACLOG_BASENAME);

  FILE* fin = fopen(fullpath, "r");
  if (!fin) return true; // no hay nada que borrar

  FILE* ftmp = fopen(tmppath, "w");
  if (!ftmp) { fclose(fin); return false; }

  size_t to_skip = n;
  int c;
  bool saw_any = false;
  bool last_was_nl = true;

  // 1) Saltar exactamente N líneas completas (cuenta '\n')
  while (to_skip > 0 && (c = fgetc(fin)) != EOF) {
    saw_any = true;
    if (c == '\r') continue;
    last_was_nl = (c == '\n');
    if (last_was_nl) to_skip--;
  }
  // Si EOF y última no tenía '\n' pero vimos algo, considérala línea
  if (to_skip > 0 && saw_any && !last_was_nl) {
    to_skip--;
  }

  // 2) Copiar el resto tal cual
  if (to_skip == 0) {
    while ((c = fgetc(fin)) != EOF) {
      fputc(c, ftmp);
    }
  }

  fflush(ftmp); fsync(fileno(ftmp));
  fclose(ftmp);
  fclose(fin);

  // 3) Reemplazo atómico
  remove(fullpath);
  rename(tmppath, fullpath);

  return true;
}

// Ejecuta purga hasta que la primera línea esté < max_age_sec (24h)
// Cierra maclog_file, borra lo viejo, y vuelve a abrir en APPEND.
static void do_purge_older_than(uint32_t max_age_sec) {
  // Asegurar que los datos en FILE actual están en disco
  if (maclog_file) {
    fflush(maclog_file);
    fsync(fileno(maclog_file));
    fclose(maclog_file);
    maclog_file = NULL;
  }

  time_t now = time(NULL);

  for (;;) {
    time_t first_ts = 0;
    if (!get_first_line_ts(first_ts)) break;      // no hay líneas
    if (first_ts > now) break;                    // timestamp futuro: no purgar
    if ((uint32_t)(now - first_ts) > max_age_sec) {
      (void)sdjson_delete_first_lines_nolock(1);  // borra 1ª línea y reevalúa
      continue;
    }
    break; // la primera línea es reciente (< max_age_sec)
  }

  // Reabrir fichero para seguir escribiendo
  (void)open_maclog_file();
  s_line_has_items = false; // nueva línea empezará sin coma
}

/* === Saneado del backlog NDJSON al arrancar ===
   - Elimina líneas vacías o obviamente corruptas (no empiezan por '{' o no acaban en '}').
   - Si hay "t":<num>, valida rango (no futuro y no prehistórico) y normaliza ms/seg al comparar.
   - Reescribe el archivo con solo líneas válidas y fuerza '\n' final.
   - No usa la cola: se ejecuta antes de arrancar el writer. */
static void sdjson_sanity_check_on_boot(void) {
  if (!useSDCard) return;

  char fullpath[96];
  snprintf(fullpath, sizeof(fullpath), "%s/%s.jsonl", mount_point, SDCARD_MACLOG_BASENAME);

  FILE* fin = fopen(fullpath, "r");
  if (!fin) {
    ESP_LOGI(TAG, "sdjson: no backlog to check on boot");
    return;
  }

  char tmppath[96];
  snprintf(tmppath, sizeof(tmppath), "%s/%s.sane", mount_point, SDCARD_MACLOG_BASENAME);
  FILE* ftmp = fopen(tmppath, "w");
  if (!ftmp) {
    ESP_LOGW(TAG, "sdjson: can't create tmp for sanity; skipping");
    fclose(fin);
    return;
  }

  // Umbrales de tiempo
  const time_t now = time(NULL);
  const bool   now_ok = (now > 1577836800);     // >= 2020-01-01
  const time_t min_ok = 1483228800;             // 2017-01-01
  const time_t max_future = now_ok ? (now + 86400) : (time_t)4102444800; // +1 día si hay hora, si no laxo

  size_t kept = 0, dropped = 0;

  String line;
  line.reserve(256);
  int c;

  auto process_line = [&](String& s) {
    while (s.length() && (s[s.length()-1] == '\r' || s[s.length()-1] == '\n' || s[s.length()-1] == ' ' || s[s.length()-1] == '\t'))
      s.remove(s.length()-1);
    int start = 0;
    while (start < (int)s.length() && (s[start] == ' ' || s[start] == '\t')) start++;

    if (start >= (int)s.length()) { dropped++; return; }
    if (s[start] != '{') { dropped++; return; }
    if (s[s.length()-1] != '}') { dropped++; return; }

    // Si hay campo t, validar
    time_t tval = 0;
    bool have_t = false;
    int idx_t = s.indexOf("\"t\"", start);
    if (idx_t < 0) idx_t = s.indexOf('t', start); // tolerante
    if (idx_t >= 0) {
      int colon = s.indexOf(':', idx_t);
      if (colon > 0) {
        int p = colon + 1;
        while (p < (int)s.length() && (s[p] == ' ' || s[p] == '\t')) p++;
        unsigned long long val = 0ULL;
        bool any = false;
        while (p < (int)s.length() && s[p] >= '0' && s[p] <= '9') {
          any = true;
          val = val * 10ULL + (unsigned)(s[p] - '0');
          p++;
        }
        if (any) {
          if (val > 100000000000ULL) { // ms
            tval = (time_t)(val / 1000ULL);
          } else {
            tval = (time_t)val;
          }
          have_t = true;
        }
      }
    }

    if (have_t) {
      if (tval < min_ok) { dropped++; return; }
      if (tval > max_future) { dropped++; return; }
    }

    fwrite(s.c_str(), 1, s.length(), ftmp);
    fputc('\n', ftmp);
    kept++;
  };

  while ((c = fgetc(fin)) != EOF) {
    if (c == '\r') continue;
    if (c == '\n') {
      if (line.length()) process_line(line);
      line = "";
      continue;
    }
    line += (char)c;
    if (line.length() > 8192) {
      process_line(line);
      line = "";
    }
  }
  if (line.length()) process_line(line);

  fflush(ftmp); fsync(fileno(ftmp));
  fclose(ftmp);
  fclose(fin);

  remove(fullpath);
  rename(tmppath, fullpath);

  ESP_LOGI(TAG, "sdjson: sanity on boot -> kept=%u dropped=%u", (unsigned)kept, (unsigned)dropped);
}

/* =================== TAREA WRITER =================== */

static void maclog_writer_task(void* arg) {
  logrec_t r;
  int n_since_flush = 0;
  for (;;) {
    if (xQueueReceive(s_log_queue, &r, portMAX_DELAY) != pdTRUE) continue;
    if (!useSDCard) continue;
    if (!maclog_file && !open_maclog_file()) continue;

    if (r.op == LOG_OP_APPEND) {
      // Si ya hay datos en la línea, anteponemos coma
      if (s_line_has_items) fputc(',', maclog_file);
      fputs(r.raw, maclog_file);
      s_line_has_items = true;

    } else if (r.op == LOG_OP_NEWLINE) {
      fputc('\n', maclog_file);
      s_line_has_items = false; // nueva línea empezará sin coma
      fflush(maclog_file);
      s_newline_ack_counter++;  // ACK: notificar a quien espera

      long pos = ftell(maclog_file);
      ESP_LOGI(TAG, "sdjson: NEWLINE escrito (pos=%ld, ack=%u)",
               pos, (unsigned)s_newline_ack_counter);

    } else if (r.op == LOG_OP_PURGE) {
      uint32_t max_age = 86400; // por defecto 24h
      if (r.raw[0]) {
        unsigned long v = strtoul(r.raw, NULL, 10);
        if (v > 0) max_age = (uint32_t)v;
      }
      ESP_LOGI(TAG, "sdjson: PURGE older than %u s (offline)", (unsigned)max_age);
      do_purge_older_than(max_age);

    } else { // LOG_OP_PING
      // No escribimos nada; sirve para asegurar que está viva
      s_newline_ack_counter++;  // Reutilizamos el contador para ping/ack
    }

    if (++n_since_flush >= SDJSON_FLUSH_EVERY) {
      fflush(maclog_file);
      n_since_flush = 0;
    }
  }
}

bool sdjson_logger_start(void) {
  if (!useSDCard) return false;
  if (!s_log_queue) s_log_queue = xQueueCreate(SDJSON_QUEUE_LEN, sizeof(logrec_t));
  if (!s_log_queue) return false;
  if (!s_log_task) {
    s_line_has_items = false;
    xTaskCreatePinnedToCore(maclog_writer_task, "maclog_writer", 4096, NULL, 1, &s_log_task, 1);
  }
  return true;
}

void sdjson_logger_stop(void) {
  if (s_log_task)   { vTaskDelete(s_log_task);   s_log_task = NULL; }
  if (s_log_queue)  { vQueueDelete(s_log_queue); s_log_queue = NULL; }
  if (maclog_file)  { fflush(maclog_file); fclose(maclog_file); maclog_file = NULL; }
}

/* compat: usado por libpax.cpp y wifi_post.cpp directamente */
extern "C" void sdcard_append_jsonl(const char *chunk) {
  if (!chunk || !s_log_queue) return;
  logrec_t r = {};
  r.op = LOG_OP_APPEND;
  strncpy(r.raw, chunk, sizeof(r.raw)-1);
  r.raw[sizeof(r.raw)-1] = '\0';

  if (xPortInIsrContext()) {
    BaseType_t hpw = pdFALSE;
    (void) xQueueSendFromISR(s_log_queue, &r, &hpw);
    if (hpw) portYIELD_FROM_ISR();
  } else {
    (void) xQueueSend(s_log_queue, &r, 0);
  }
}

/* Cerrar la línea actual (asíncrono) */
extern "C" void sdcard_newline(void) {
  if (!s_log_queue) return;
  logrec_t r = {};
  r.op = LOG_OP_NEWLINE;

  if (xPortInIsrContext()) {
    BaseType_t hpw = pdFALSE;
    (void) xQueueSendFromISR(s_log_queue, &r, &hpw);
    if (hpw) portYIELD_FROM_ISR();
  } else {
    (void) xQueueSend(s_log_queue, &r, 0);
  }
}

/* Ping simple a la tarea writer (para confirmar que está viva) */
static void sdcard_ping_async(void) {
  if (!s_log_queue) return;
  logrec_t r = {};
  r.op = LOG_OP_PING;
  (void) xQueueSend(s_log_queue, &r, 0);
}

/* Cerrar la línea actual y ESPERAR a que el '\n' esté realmente escrito */
extern "C" bool sdcard_newline_sync(uint32_t timeout_ms) {
  if (!s_log_queue) return false;
  uint32_t start_ack = s_newline_ack_counter;

  // Enviamos la orden de salto de línea
  sdcard_newline();

  // Espera activa simple (ticks de FreeRTOS)
  uint32_t waited = 0;
  const uint32_t step = 5; // ms
  while (waited < timeout_ms) {
    if (s_newline_ack_counter != start_ack) return true; // se procesó
    vTaskDelay(pdMS_TO_TICKS(step));
    waited += step;

    // por si la cola estaba llena y no avanzó, intentamos un ping
    sdcard_ping_async();
  }
  return false; // timeout
}

/*========================
 *  Montaje + CSV clásico
 *========================*/

bool sdcard_init(bool create) {
  esp_err_t ret;
  esp_vfs_fat_mount_config_t mount_config = {.format_if_mount_failed = false,
                                             .max_files = 5};

  ESP_LOGI(TAG, "looking for SD-card...");

#if (HAS_SDCARD == 1)
  sdmmc_host_t host = SDSPI_HOST_DEFAULT();
  spi_bus_config_t bus_cfg = {
      .mosi_io_num = (gpio_num_t)SDCARD_MOSI,
      .miso_io_num = (gpio_num_t)SDCARD_MISO,
      .sclk_io_num = (gpio_num_t)SDCARD_SCLK,
      .quadwp_io_num = -1,
      .quadhd_io_num = -1,
      .max_transfer_sz = 4000,
  };
  sdspi_device_config_t slot_config = SDSPI_DEVICE_CONFIG_DEFAULT();
  slot_config.gpio_cs = (gpio_num_t)SDCARD_CS;

  ret = spi_bus_initialize(SPI_HOST, &bus_cfg, 1);
  if (ret != ESP_OK) {
    ESP_LOGE(TAG, "failed to initialize SPI bus");
    return false;
  }
  ret = esp_vfs_fat_sdspi_mount(mount_point, &host, &slot_config, &mount_config, &card);

#elif (HAS_SDCARD == 2)
  sdmmc_host_t host = SDMMC_HOST_DEFAULT();
  sdmmc_slot_config_t slot_config = SDCARD_SLOTCONFIG;
  slot_config.width = SDCARD_SLOTWIDTH;
  slot_config.flags |= SDCARD_PULLUP;
  ret = esp_vfs_fat_sdmmc_mount(mount_point, &host, &slot_config, &mount_config, &card);
#endif

  if (ret != ESP_OK) {
    if (ret == ESP_FAIL) {
      ESP_LOGE(TAG, "failed to mount filesystem");
    } else {
      ESP_LOGI(TAG, "No SD-card found (%d)", ret);
    }
    return false;
  }

  useSDCard = true;
  ESP_LOGI(TAG, "filesystem mounted");
  sdmmc_card_print_info(stdout, card);

  // *** CAMBIO MINIMO: NO crear el fichero clásico paxcounter_*.json ***
  // (Se deja data_file en nullptr para que sdcardWriteData no escriba nada)
  data_file = nullptr;

#if (SDLOGGING)
  char bufferFilename[64];
  snprintf(bufferFilename, sizeof(bufferFilename), "/%s.log", SDCARD_FILE_NAME);
  if (openFile(&log_file, bufferFilename)) {
    ESP_LOGI(TAG, "redirecting serial output to SD-card");
    esp_log_set_vprintf(&print_to_sd_card);
  } else {
    useSDCard = false;
  }
#endif

  // Sanea backlog NDJSON al arranque ANTES de iniciar el writer
  sdjson_sanity_check_on_boot();

  sdjson_logger_start();
  return useSDCard;
}

void sdcard_flush(void) {
  if (data_file) fsync(fileno(data_file));
#if (SDLOGGING)
  if (log_file) fsync(fileno(log_file));
#endif
  if (maclog_file) fsync(fileno(maclog_file));
}

void sdcard_close(void) {
  if (!useSDCard) return;
  ESP_LOGI(TAG, "closing SD-card");
  sdcard_flush();
#if (SDLOGGING)
  ESP_LOGI(TAG, "redirect console back to serial output");
  esp_log_set_vprintf(&vprintf);
#endif
  sdjson_logger_stop();
  fcloseall();
  esp_vfs_fat_sdcard_unmount(mount_point, card);
  ESP_LOGI(TAG, "SD-card unmounted");
}

void sdcardWriteData(uint16_t noWifi, uint16_t noBle,
                     __attribute__((unused)) uint16_t voltage) {
  // *** CAMBIO MINIMO: no escribir si no hay fichero clásico abierto ***
  if (!useSDCard || data_file == nullptr) return;

  char timeBuffer[21];
  time_t t = time(NULL);
  struct tm tt;
  gmtime_r(&t, &tt);
  strftime(timeBuffer, sizeof(timeBuffer), "%FT%TZ", &tt);

#if (HAS_SDS011)
  sdsStatus_t sds;
#endif

  ESP_LOGI(TAG, "writing data to SD-card");

  fprintf(data_file, "%s", timeBuffer);
  fprintf(data_file, ",%d,%d", noWifi, noBle);
#if (defined BAT_MEASURE_ADC || defined HAS_PMU)
  fprintf(data_file, ",%d", voltage);
#endif
#if (HAS_SDS011)
  sds011_store(&sds);
  fprintf(data_file, ",%5.1f,%4.1f", sds.pm10 / 10, sds.pm25 / 10);
#endif
  fprintf(data_file, "\n");
}

/*==========================================
 *  Helpers de lote para wifi_post.cpp
 *==========================================*/

extern "C" bool sdjson_read_batch(String &outEventsArray,
                                  size_t max_lines,
                                  size_t max_bytes,
                                  size_t &out_lines_read)
{
  outEventsArray = "[]";
  out_lines_read = 0;
  if (!useSDCard) return false;

  // Paramos el writer para consistencia
  sdjson_logger_stop();

  char fullpath[96];
  snprintf(fullpath, sizeof(fullpath), "%s/%s.jsonl", mount_point, SDCARD_MACLOG_BASENAME);

  FILE* fin = fopen(fullpath, "r");
  if (!fin) {
    // no existe el backlog aún
    sdjson_logger_start();
    return true;
  }

  String arr;
  arr.reserve(256);
  arr = "[";

  // Lectura robusta por caracteres
  bool first_line = true;
  size_t count = 0;
  size_t bytes_acc = 1; // '['

  int c;
  bool line_has_data = false;
  String line;

  while ((c = fgetc(fin)) != EOF) {
    if (c == '\r') continue;
    if (c == '\n') {
      if (!line_has_data) continue;

      size_t add_bytes = (first_line ? 0 : 1) + line.length();
      if ((bytes_acc + add_bytes + 1) > max_bytes) break;   // +1 por ']'
      if (count >= max_lines) break;

      if (!first_line) arr += ",";
      arr += line;

      bytes_acc += add_bytes;
      first_line = false;
      ++count;

      line = "";
      line_has_data = false;
      continue;
    } else {
      line_has_data = true;
      line += (char)c;
    }
  }
  // última línea sin \n
  if (line_has_data && count < max_lines) {
    size_t add_bytes = (first_line ? 0 : 1) + line.length();
    if ((bytes_acc + add_bytes + 1) <= max_bytes) {
      if (!first_line) arr += ",";
      arr += line;
      ++count;
    }
  }

  fclose(fin);

  arr += "]";
  outEventsArray = arr;
  out_lines_read = count;

  // Reanudamos el writer
  sdjson_logger_start();
  return true;
}

/* BORRADO ROBUSTO DE LAS PRIMERAS N LÍNEAS (maneja líneas largas) */
extern "C" bool sdjson_delete_first_lines(size_t n) {
  if (!useSDCard || n == 0) return true;

  // Detenemos el writer para tener control exclusivo del archivo
  sdjson_logger_stop();

  char fullpath[96];
  snprintf(fullpath, sizeof(fullpath), "%s/%s.jsonl", mount_point, SDCARD_MACLOG_BASENAME);
  char tmppath[96];
  snprintf(tmppath,  sizeof(tmppath),  "%s/%s.tmp",   mount_point, SDCARD_MACLOG_BASENAME);

  FILE* fin = fopen(fullpath, "r");
  if (!fin) { sdjson_logger_start(); return true; }

  FILE* ftmp = fopen(tmppath, "w");
  if (!ftmp) { fclose(fin); sdjson_logger_start(); return false; }

  size_t to_skip = n;
  int c;
  bool saw_any = false;
  bool last_was_nl = true;

  // 1) Saltar exactamente N líneas completas (cuenta '\n')
  while (to_skip > 0 && (c = fgetc(fin)) != EOF) {
    saw_any = true;
    if (c == '\r') continue;
    last_was_nl = (c == '\n');
    if (last_was_nl) to_skip--;
  }
  // Si EOF y última línea no tenía '\n' pero vimos algo, considérala línea
  if (to_skip > 0 && saw_any && !last_was_nl) {
    to_skip--;
  }

  // 2) Copiar el resto tal cual
  if (to_skip == 0) {
    while ((c = fgetc(fin)) != EOF) {
      fputc(c, ftmp);
    }
  }

  fflush(ftmp); fsync(fileno(ftmp));
  fclose(ftmp);
  fclose(fin);

  // 3) Reemplazo atómico
  remove(fullpath);
  rename(tmppath, fullpath);

  // Reanudamos el writer
  sdjson_logger_start();
  return true;
}

/* API pública: pedir PURGA al writer (se encola, sin carreras) */
extern "C" void sdjson_request_purge_older_than(uint32_t max_age_sec) {
  if (!s_log_queue) return;
  logrec_t r = {};
  r.op = LOG_OP_PURGE;
  snprintf(r.raw, sizeof(r.raw), "%lu", (unsigned long)max_age_sec);

  if (xPortInIsrContext()) {
    BaseType_t hpw = pdFALSE;
    (void)xQueueSendFromISR(s_log_queue, &r, &hpw);
    if (hpw) portYIELD_FROM_ISR();
  } else {
    (void)xQueueSend(s_log_queue, &r, 0);
  }
}

#endif // HAS_SDCARD

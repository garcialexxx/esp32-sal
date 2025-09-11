// src/net_time.cpp
#include "net_time.h"
#include <time.h>

#ifdef ARDUINO
  #include <Arduino.h>   // para delay()
#endif

// Puedes cambiar estos con build_flags: -DNET_TZ="\"...\""
#ifndef NET_TZ
#define NET_TZ   "CET-1CEST,M3.5.0/2,M10.5.0/3"   // Europa/Madrid con DST
#endif
#ifndef NET_NTP1
#define NET_NTP1 "pool.ntp.org"
#endif
#ifndef NET_NTP2
#define NET_NTP2 "time.nist.gov"
#endif
#ifndef NET_NTP3
#define NET_NTP3 "time.google.com"
#endif

// Inicializa NTP + zona horaria (llamar una vez tras tener Wi-Fi)
void netTimeInit(void) {
  // Disponible en Arduino-ESP32 y ESP-IDF
  configTzTime(NET_TZ, NET_NTP1, NET_NTP2, NET_NTP3);
}

// ¿Ya hay hora real de Internet?
bool netTimeReady(void) {
  // “Época razonable”: > 2023-11-14 aprox.
  return time(nullptr) > 1700000000;
}

// Espera a que se sincronice el reloj NTP (ms)
bool netTimeWaitSync(unsigned long timeout_ms) {
  unsigned long waited = 0;
  const unsigned long step = 200;
  while (!netTimeReady() && waited < timeout_ms) {
  #ifdef ARDUINO
    delay(step);
  #else
    vTaskDelay(step / portTICK_PERIOD_MS);
  #endif
    waited += step;
  }
  return netTimeReady();
}

// Epoch UTC (segundos desde 1970)
time_t netEpoch(void) {
  return time(nullptr);
}

// Hora local formateada "YYYY-MM-DD HH:MM:SS"
void netLocalString(char* out, size_t n) {
  time_t now = time(nullptr);
  if (now <= 0) { snprintf(out, n, "NO_TIME"); return; }
  struct tm tmnow;
  localtime_r(&now, &tmnow);
  strftime(out, n, "%Y-%m-%d %H:%M:%S", &tmnow);
}

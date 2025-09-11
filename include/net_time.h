// include/net_time.h
#pragma once
#include <stddef.h>
#include <time.h>


#ifdef __cplusplus
extern "C" {
#endif

// Inicializa NTP y zona horaria (llamar una vez tras tener Wi-Fi)
void   netTimeInit(void);

// ¿Ya hay hora “real” de Internet?
bool   netTimeReady(void);

// Espera a que se sincronice (ms). Devuelve true si listo.
bool   netTimeWaitSync(unsigned long timeout_ms);

// Epoch UTC (segundos desde 1970). Usa time(NULL) por debajo.
time_t netEpoch(void);

// Hora local formateada "YYYY-MM-DD HH:MM:SS" (Madrid)
void   netLocalString(char* out, size_t n);

#ifdef __cplusplus
}
#endif

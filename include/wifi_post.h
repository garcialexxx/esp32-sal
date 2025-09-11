
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

// Inicializa cola/tarea HTTP (idempotente)
void wifi_post_init(void);

// Encola un registro (wifi, ble, timestamp) para POST
void wifi_post_counts(int wifi, time_t ts);

#ifdef __cplusplus
}
#endif

#include "main.h"
#include <WiFi.h>

#include "paxcounter.conf"
#include <Ticker.h> 
#include "sdcard.h"
#include "wifi_post.h"      



RTC_DATA_ATTR static bool firstBoot = true;
Ticker initialResetTimer;
Ticker normalResetTimer;
void resetSystemTimeToEpoch0() {
  struct timeval tv;
  tv.tv_sec = 0;   
  tv.tv_usec = 0;
  settimeofday(&tv, nullptr);
}

// Ahora: reinicia solo si hay Wi-Fi; si no, lo deja para el próximo ciclo
void onNormalReset() {
  if (WiFi.status() == WL_CONNECTED) {
    Serial.println("Reinicio periódico: Wi-Fi OK → reiniciando.");
    esp_restart();
  } else {
    Serial.println("Reinicio periódico: sin Wi-Fi, se pospone hasta el próximo ciclo.");
    // No reprogramamos nada; el Ticker attach ya volverá a llamar en 2 h
  }
}




char clientId[20] = {0}; // unique ClientID

void setup() {
  char features[100] = "";
  resetSystemTimeToEpoch0();
#ifdef DISABLE_BROWNOUT
  (*((uint32_t volatile *)ETS_UNCACHED_ADDR((DR_REG_RTCCNTL_BASE + 0xd4)))) = 0;
#endif

  uint8_t mac[6];
  esp_read_mac(mac, ESP_MAC_WIFI_STA);

  const uint32_t hashedmac = myhash((const char *)mac, 6);
  snprintf(clientId, 20, "paxcounter_%08x", hashedmac);

#if (VERBOSE)
  Serial.begin(115200);
  esp_log_level_set("*", ESP_LOG_VERBOSE);
#else
  esp_log_level_set("*", ESP_LOG_NONE);
#endif

#if (HAS_SDCARD)
  if (sdcard_init())
    strcat_P(features, " SD");
#endif

  do_after_reset();

  ESP_LOGI(TAG, "Starting %s v%s (runmode=%d / restarts=%d)", clientId,
           PROGVERSION, RTC_runmode, RTC_restarts);
  ESP_LOGI(TAG, "code build date: %d", compileTime());

#if (VERBOSE)
  if (RTC_runmode == RUNMODE_POWERCYCLE) {
    esp_chip_info_t chip_info;
    esp_chip_info(&chip_info);
    ESP_LOGI(TAG,
             "This is ESP32 chip with %d CPU cores, WiFi%s%s, silicon revision "
             "%d, %dMB %s Flash",
             chip_info.cores,
             (chip_info.features & CHIP_FEATURE_BT) ? "/BT" : "",
             (chip_info.features & CHIP_FEATURE_BLE) ? "/BLE" : "",
             chip_info.revision, spi_flash_get_chip_size() / (1024 * 1024),
             (chip_info.features & CHIP_FEATURE_EMB_FLASH) ? "embedded"
                                                           : "external");
    ESP_LOGI(TAG, "Internal Total heap %d, internal Free Heap %d",
             ESP.getHeapSize(), ESP.getFreeHeap());
#ifdef BOARD_HAS_PSRAM
    ESP_LOGI(TAG, "SPIRam Total heap %d, SPIRam Free Heap %d",
             ESP.getPsramSize(), ESP.getFreePsram());
#endif
    ESP_LOGI(TAG, "ChipRevision %d, Cpu Freq %d, SDK Version %s",
             ESP.getChipRevision(), ESP.getCpuFreqMHz(), ESP.getSdkVersion());
    ESP_LOGI(TAG, "Flash Size %d, Flash Speed %d", ESP.getFlashChipSize(),
             ESP.getFlashChipSpeed());
    ESP_LOGI(TAG, "Wifi/BT software coexist version %s",
             esp_coex_version_get());
    ESP_LOGI(TAG, "Wifi STA MAC: %s",
             WiFi.macAddress().c_str());
#if (HAS_LORA)
// (opcional) estos logs también son de LoRa:
ESP_LOGI(TAG, "IBM LMIC version %d.%d.%d", LMIC_VERSION_MAJOR,
         LMIC_VERSION_MINOR, LMIC_VERSION_BUILD);
ESP_LOGI(TAG, "Arduino LMIC version %d.%d.%d.%d",
         ARDUINO_LMIC_VERSION_GET_MAJOR(ARDUINO_LMIC_VERSION),
         ARDUINO_LMIC_VERSION_GET_MINOR(ARDUINO_LMIC_VERSION),
         ARDUINO_LMIC_VERSION_GET_PATCH(ARDUINO_LMIC_VERSION),
         ARDUINO_LMIC_VERSION_GET_LOCAL(ARDUINO_LMIC_VERSION));

showLoraKeys();
#endif
#if (HAS_GPS)
    ESP_LOGI(TAG, "TinyGPS+ version %s", TinyGPSPlus::libraryVersion());
#endif
  }
#endif // VERBOSE

  i2c_init();

#ifdef EXT_POWER_SW
  pinMode(EXT_POWER_SW, OUTPUT);
  digitalWrite(EXT_POWER_SW, EXT_POWER_ON);
  strcat_P(features, " VEXT");
#endif

#if defined HAS_PMU || defined HAS_IP5306
#ifdef HAS_PMU
  PMU_init();
#elif defined HAS_IP5306
  IP5306_init();
#endif
  strcat_P(features, " PMU");
#endif

#if (HAS_SDCARD)
  if (RTC_runmode == RUNMODE_POWERCYCLE)
    i2c_scan();
#endif

#ifdef HAS_DISPLAY
  strcat_P(features, " DISP");
  DisplayIsOn = cfg.screenon;
  dp_init(RTC_runmode == RUNMODE_POWERCYCLE ? true : false);
#endif

#ifdef BOARD_HAS_PSRAM
  _ASSERT(psramFound());
  ESP_LOGI(TAG, "PSRAM found and initialized");
  strcat_P(features, " PSRAM");
#endif

#ifdef BAT_MEASURE_EN
  pinMode(BAT_MEASURE_EN, OUTPUT);
#endif

#ifdef HAS_RGB_LED
  rgb_led_init();
  strcat_P(features, " RGB");
#endif

#if (HAS_LED != NOT_A_PIN)
  pinMode(HAS_LED, OUTPUT);
  strcat_P(features, " LED");
#ifdef LED_POWER_SW
  pinMode(LED_POWER_SW, OUTPUT);
  digitalWrite(LED_POWER_SW, LED_POWER_ON);
#endif
#ifdef HAS_TWO_LED
  pinMode(HAS_TWO_LED, OUTPUT);
  strcat_P(features, " LED2");
#endif
#ifdef HAS_RGB_LED
  switch_LED(LED_ON);
#endif
#endif

#if (HAS_LED != NOT_A_PIN) || defined(HAS_RGB_LED)
  ESP_LOGI(TAG, "Starting LED Controller...");
  xTaskCreatePinnedToCore(ledLoop,
                          "ledloop",
                          1024,
                          (void *)1,
                          1,
                          &ledLoopTask,
                          1);
#endif

#ifdef HAS_ANTENNA_SWITCH
  strcat_P(features, " ANT");
  antenna_init();
  antenna_select(cfg.wifiant);
#endif

#if defined BAT_MEASURE_ADC || defined HAS_PMU || defined HAS_IP5306
  strcat_P(features, " BATT");
  calibrate_voltage();
  batt_level = read_battlevel();
#ifdef HAS_IP5306
  printIP5306Stats();
#endif
#endif

#if (USE_OTA)
  strcat_P(features, " OTA");
  if (RTC_runmode == RUNMODE_UPDATE)
    start_ota_update();
#endif

#if (BOOTMENU)
  if (RTC_runmode == RUNMODE_POWERCYCLE)
    start_boot_menu();
#endif

  if (RTC_runmode == RUNMODE_MAINTENANCE)
    start_boot_menu();

  ESP_LOGI(TAG, "Starting libpax...");
  struct libpax_config_t configuration;
  libpax_default_config(&configuration);

  strcpy(configuration.wifi_my_country_str, WIFI_MY_COUNTRY);
  configuration.wificounter = cfg.wifiscan;
  configuration.wifi_channel_map = cfg.wifichanmap;
  configuration.wifi_channel_switch_interval = cfg.wifichancycle;
  configuration.wifi_rssi_threshold = cfg.rssilimit;
  ESP_LOGI(TAG, "WIFISCAN: %s", cfg.wifiscan ? "on" : "off");

  configuration.blecounter = cfg.blescan;
  configuration.blescantime = cfg.blescantime;
  configuration.ble_rssi_threshold = cfg.rssilimit;
  ESP_LOGI(TAG, "BLESCAN: %s", cfg.blescan ? "on" : "off");

  int config_update = libpax_update_config(&configuration);
  if (config_update != 0) {
    ESP_LOGE(TAG, "Error in libpax configuration.");
  } else {
    init_libpax();
  }

  ESP_LOGI(TAG, "Starting rcommand interpreter...");
  rcmd_init();

#if (HAS_GPS)
  strcat_P(features, " GPS");
  if (gps_init()) {
    ESP_LOGI(TAG, "Starting GPS Feed...");
    xTaskCreatePinnedToCore(gps_loop,
                            "gpsloop",
                            8192,
                            (void *)1,
                            1,
                            &GpsTask,
                            1);
  }
#endif

#if (HAS_SENSORS)
#if (HAS_SENSOR_1)
  strcat_P(features, " SENS(1)");
  sensor_init();
#endif
#if (HAS_SENSOR_2)
  strcat_P(features, " SENS(2)");
  sensor_init();
#endif
#if (HAS_SENSOR_3)
  strcat_P(features, " SENS(3)");
  sensor_init();
#endif
#endif

#if (HAS_LORA)
  strcat_P(features, " LORA");
  _ASSERT(lmic_init() == ESP_OK);
#endif

#ifdef HAS_SPI
  strcat_P(features, " SPI");
  _ASSERT(spi_init() == ESP_OK);
#endif

#ifdef HAS_MQTT
  strcat_P(features, " MQTT");
  _ASSERT(mqtt_init() == ESP_OK);
#endif

#if (HAS_SDS011)
  ESP_LOGI(TAG, "init fine-dust-sensor");
  if (sds011_init())
    strcat_P(features, " SDS");
#endif

#ifdef HAS_MATRIX_DISPLAY
  strcat_P(features, " LED_MATRIX");
  MatrixDisplayIsOn = cfg.screenon;
  init_matrix_display();
#endif

#if PAYLOAD_ENCODER == 1
  strcat_P(features, " PLAIN");
#elif PAYLOAD_ENCODER == 2
  strcat_P(features, " PACKED");
#elif PAYLOAD_ENCODER == 3
  strcat_P(features, " LPPDYN");
#elif PAYLOAD_ENCODER == 4
  strcat_P(features, " LPPPKD");
#endif

#ifdef HAS_RTC
  strcat_P(features, " RTC");
  _ASSERT(rtc_init());
#endif

#if defined HAS_DCF77
  strcat_P(features, " DCF77");
#endif

#if defined HAS_IF482
  strcat_P(features, " IF482");
#endif

  ESP_LOGI(TAG, "Starting Interrupt Handler...");
  xTaskCreatePinnedToCore(irqHandler,
                          "irqhandler",
                          4096,
                          (void *)1,
                          4,
                          &irqHandlerTask,
                          1);

#if (HAS_BME)
#ifdef HAS_BME680
  strcat_P(features, " BME680");
#elif defined HAS_BME280
  strcat_P(features, " BME280");
#elif defined HAS_BMP180
  strcat_P(features, " BMP180");
#elif defined HAS_BMP280
  strcat_P(features, " BMP280");
#endif
  if (bme_init())
    ESP_LOGI(TAG, "BME sensor initialized");
  else {
    ESP_LOGE(TAG, "BME sensor could not be initialized");
    cfg.payloadmask &= (uint8_t)~MEMS_DATA;
  }
#endif

#ifdef HAS_DISPLAY
  dp_clear();
  dp_contrast(DISPLAYCONTRAST);
  displayIRQ = timerBegin(0, 80, true);
  timerAttachInterrupt(displayIRQ, &DisplayIRQ, false);
  timerAlarmWrite(displayIRQ, DISPLAYREFRESH_MS * 1000, true);
  timerAlarmEnable(displayIRQ);
#endif

#ifdef HAS_MATRIX_DISPLAY
  matrixDisplayIRQ = timerBegin(3, 80, true);
  timerAttachInterrupt(matrixDisplayIRQ, &MatrixDisplayIRQ, false);
  timerAlarmWrite(matrixDisplayIRQ, MATRIX_DISPLAY_SCAN_US, true);
  timerAlarmEnable(matrixDisplayIRQ);
#endif

#ifdef HAS_BUTTON
  strcat_P(features, " BTN_");
#ifdef BUTTON_PULLUP
  strcat_P(features, "PU");
#else
  strcat_P(features, "PD");
#endif
  button_init();
#endif

#if ((HAS_LORA_TIME) || (HAS_GPS) || defined HAS_RTC)
  time_init();
  strcat_P(features, " TIME");
#endif

  cyclicTimer.attach(HOMECYCLE, setCyclicIRQ);

  ESP_LOGI(TAG, "Features:%s", features);

  RTC_runmode = RUNMODE_NORMAL;



  // Inicializa el módulo Wi-Fi/POST (no envía nada si nadie encola mensajes)
  wifi_post_init();

  // Reinicios periódicos 
  normalResetTimer.attach(86400, onNormalReset); // 7200 s = 2h

  vTaskDelete(NULL);
}

void loop() {
  vTaskDelete(NULL);
}

package config

import (
	"log"
	"os"
	"sync"

	"github.com/joho/godotenv"
)

var once sync.Once

func LoadEnv() {
	once.Do(func() {
		if err := godotenv.Load(); err != nil {
			log.Println("No se pudo cargar el archivo .env, usando variables de entorno del sistema.")
		}
	})
}

func GetEnv(key, fallback string) string {
	LoadEnv()
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

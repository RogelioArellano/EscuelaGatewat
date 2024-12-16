package app

import (
	"SchoolGateway/config"
	"SchoolGateway/service"
	"log"
)

func StartApplication() {
	// Inicializar el consumidor de Kafka
	kafkaConsumer, err := config.InitKafkaConsumer()
	if err != nil {
		log.Fatal("Error al inicializar el consumidor de Kafka:", err)
	}

	// Inicializar el servidor TCP
	listener, err := config.NewSocketClient()
	if err != nil {
		log.Fatal("Error al inicializar el servidor TCP:", err)
	}
	defer listener.Close()

	// Crear instancia del servicio de Sockets
	socketService := service.NewService(kafkaConsumer)

	// Iniciar una goroutine para manejar conexiones entrantes
	go socketService.HandleConnections(listener)

	// Iniciar una goroutine para manejar mensajes de Kafka
	go socketService.ConsumeKafkaMessages()

	// Iniciar una goroutine para manejar mensajes a los clientes
	go socketService.HandleMessages()

	// Mantener la aplicación en ejecución
	select {}
}

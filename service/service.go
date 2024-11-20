package service

import (
	"SchoolGateway/models"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

// Modelo del servicio de SchoolGateway Service
type Service struct {
	KafkaConsumer sarama.Consumer
	Clients       map[net.Conn]bool
	Broadcast     chan []byte
	Mutex         sync.Mutex
}

// NewService crea una instancia del servicio
func NewService(kafkaConsumer sarama.Consumer) *Service {
	return &Service{
		KafkaConsumer: kafkaConsumer,
		Clients:       make(map[net.Conn]bool),
		Broadcast:     make(chan []byte),
	}
}

// HandleConnections permite manipular las conexiones
func (s *Service) HandleConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error al aceptar conexión:", err)
			continue
		}

		s.Mutex.Lock()
		s.Clients[conn] = true
		s.Mutex.Unlock()

		log.Println("Cliente conectado:", conn.RemoteAddr())

		// Iniciar una goroutine para manejar la comunicación con el cliente
		go s.HandleClient(conn)
	}
}

func (s *Service) HandleClient(conn net.Conn) {
	defer func() {
		conn.Close()
		s.Mutex.Lock()
		delete(s.Clients, conn)
		s.Mutex.Unlock()
		log.Println("Cliente desconectado:", conn.RemoteAddr())
	}()

	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Error al leer del cliente:", err)
			}
			break
		}
		message := buffer[:n]
		// Procesar el mensaje recibido si es necesario
		log.Printf("Mensaje recibido del cliente %s: %v", conn.RemoteAddr(), message)
		// Puedes agregar lógica para responder al cliente si lo deseas
	}
}

func (s *Service) HandleMessages() {
	for {
		message := <-s.Broadcast
		s.Mutex.Lock()
		for client := range s.Clients {
			_, err := client.Write(message)
			if err != nil {
				log.Println("Error al enviar mensaje al cliente:", err)
				client.Close()
				delete(s.Clients, client)
			}
		}
		s.Mutex.Unlock()
	}
}

func (s *Service) ConsumeKafkaMessages() {
	topic := os.Getenv("KAFKA_TOPIC")
	partitionConsumer, err := s.KafkaConsumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Println("Error al iniciar consumo de partición:", err)
		return
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			log.Printf("Mensaje recibido de Kafka: %s", string(msg.Value))
			// Procesar el mensaje y transformarlo a arreglo de bytes
			var instruccion models.InstruccionEnvio
			err := json.Unmarshal(msg.Value, &instruccion)
			if err != nil {
				log.Println("Error al deserializar mensaje:", err)
				continue
			}
			// Convertir la instrucción a arreglo de bytes
			byteData, err := json.Marshal(instruccion)
			if err != nil {
				log.Println("Error al serializar instrucción:", err)
				continue
			}
			// Enviar el arreglo de bytes al canal Broadcast
			s.Broadcast <- byteData
		case err := <-partitionConsumer.Errors():
			log.Println("Error en consumo de Kafka:", err)
		}
	}
}

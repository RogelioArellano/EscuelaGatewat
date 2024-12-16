package service

import (
	"SchoolGateway/models"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"github.com/IBM/sarama"
)

// Modelo del servicio
type Service struct {
	KafkaConsumer sarama.Consumer
	Clients       map[net.Conn]bool // Mapa para rastrear
	Broadcast     chan []byte 
	Mutex         sync.Mutex  // Mutex para proteger el acceso concurrente
}

// NewService crea una instancia del servicio
func NewService(kafkaConsumer sarama.Consumer) *Service {
	return &Service{
		KafkaConsumer: kafkaConsumer,
		Clients:       make(map[net.Conn]bool),
		Broadcast:     make(chan []byte),
	}
}

// HandleConnections manejar las conexiones
func (s *Service) HandleConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error al aceptar conexión:", err)
			continue
		}

		// Agregar el cliente a la lista de conexiones activas
		s.Mutex.Lock()
		s.Clients[conn] = true
		s.Mutex.Unlock()

		log.Println("Cliente conectado:", conn.RemoteAddr())

		// Crear una goroutine para manejar la comunicación con el cliente
		go s.HandleClient(conn)
	}
}

// HandleClient maneja la comunicación con un cliente conectado
func (s *Service) HandleClient(conn net.Conn) {
	defer func() {
		// Cerrar la conexión y eliminar al cliente
		conn.Close()
		s.Mutex.Lock()
		delete(s.Clients, conn)
		s.Mutex.Unlock()
		log.Println("Cliente desconectado:", conn.RemoteAddr())
	}()

	buffer := make([]byte, 1024) // Buffer para leer datos del cliente
	for {
		// Leer datos del cliente
		n, err := conn.Read(buffer)
		if err != nil {
			if err != io.EOF {
				log.Println("Error al leer del cliente:", err)
			}
			break
		}
		message := buffer[:n] // Extraer los datos leídos
		log.Printf("Mensaje recibido del cliente %s: %v", conn.RemoteAddr(), message)
	}
}

// HandleMessages difunde mensajes a todos los clientes conectados
func (s *Service) HandleMessages() {
	for {
		message := <-s.Broadcast // Recibir un mensaje del canal Broadcast
		s.Mutex.Lock()
		for client := range s.Clients {
			_, err := client.Write(message) // Enviar el mensaje a cada cliente conectado
			if err != nil {
				log.Println("Error al enviar mensaje al cliente:", err)
				client.Close()
				delete(s.Clients, client) // Eliminar al cliente si hay un error
			}
		}
		s.Mutex.Unlock()
	}
}

// ConsumeKafkaMessages consume mensajes de Kafka y los procesa
func (s *Service) ConsumeKafkaMessages() {
	topic := os.Getenv("KAFKA_TOPIC") // Obtener el tópico de Kafka desde las variables de entorno
	partitionConsumer, err := s.KafkaConsumer.ConsumePartition(topic, 0, sarama.OffsetNewest)
	if err != nil {
		log.Println("Error al iniciar consumo de partición:", err)
		return
	}
	defer partitionConsumer.Close()

	for {
		select {
		case msg := <-partitionConsumer.Messages(): // Recibir mensaje de Kafka
			log.Printf("Mensaje recibido de Kafka: %s", string(msg.Value))

			// Deserializar el mensaje de Kafka en una estructura de Go
			var instruccion models.InstruccionEnvio
			err := json.Unmarshal(msg.Value, &instruccion)
			if err != nil {
				log.Println("Error al deserializar mensaje:", err)
				continue
			}

			// Convertir la estructura en bytes en formato Big Endian
			byteData, err := instruccionToBytes(instruccion)
			if err != nil {
				log.Println("Error al convertir instrucción a Big Endian bytes:", err)
				continue
			}

			// Enviar los bytes al canal Broadcast
			s.Broadcast <- byteData
		case err := <-partitionConsumer.Errors(): // Manejar errores de Kafka
			log.Println("Error en consumo de Kafka:", err)
		}
	}
}

// instruccionToBigEndianBytes convierte una instrucción a un arreglo de bytes en formato Big Endian
func instruccionToBytes(instruccion models.InstruccionEnvio) ([]byte, error) {
	buffer := new(bytes.Buffer) // Crear un buffer para construir los datos

	// Escribir InstruccionID en bytes
	err := binary.Write(buffer, binary.BigEndian, instruccion.InstruccionID)
	if err != nil {
		return nil, err
	}

	// Escribir FechaOperacion como bytes
	err = binary.Write(buffer, binary.BigEndian, []byte(instruccion.FechaOperacion))
	if err != nil {
		return nil, err
	}

	// Escribir ClaveEmisor en bytes
	err = binary.Write(buffer, binary.BigEndian, instruccion.ClaveEmisor)
	if err != nil {
		return nil, err
	}

	// Escribir FolioConsecutivo en bytes
	err = binary.Write(buffer, binary.BigEndian, instruccion.FolioConsecutivo)
	if err != nil {
		return nil, err
	}

	// Escribir NumAltaEstudiantes en bytes
	err = binary.Write(buffer, binary.BigEndian, instruccion.NumAltaEstudiantes)
	if err != nil {
		return nil, err
	}

	// Serializar InfoAdicional como JSON y escribirlo en el buffer
	infoAdicionalBytes, err := json.Marshal(instruccion.InfoAdicional)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buffer, binary.BigEndian, infoAdicionalBytes)
	if err != nil {
		return nil, err
	}

	// Retornar los datos como arreglo de bytes
	return buffer.Bytes(), nil
}

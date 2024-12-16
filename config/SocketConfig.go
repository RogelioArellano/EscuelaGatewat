package config

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

const (
	SepHost    = "localhost"     // Reemplazar con la dirección real del servidor
	SepPort    = "7776"          // Reemplazar con el puerto real
	ConnectTimeout = 5 * time.Second // Timeout de 5 segundos para la conexión
)

type SocketClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

// NewSocketClient crea una nueva instancia de SocketClient y establece la conexión con Banxico
func NewSocketClient() (*SocketClient, error) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%s", SepHost, SepPort), ConnectTimeout)
	if err != nil {
		return nil, err
	}

	return &SocketClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		writer: bufio.NewWriter(conn),
	}, nil
}

// Send envía un mensaje a Banxico a través del socket
func (s *SocketClient) Send(message []byte) error {
	totalBytesWritten := 0
	messageLength := len(message)

	for totalBytesWritten < messageLength {
		n, err := s.writer.Write(message[totalBytesWritten:])
		if err != nil {
			log.Printf("Error al enviar mensaje: %v", err)
			return err
		}
		totalBytesWritten += n
	}

	return s.writer.Flush()

}

func (s *SocketClient) receive() ([]byte, error) {
	var buffer bytes.Buffer
	temp := make([]byte, 1024) // Un buffer temporal para leer los datos
	for {
		n, err := s.reader.Read(temp)
		if err != nil {
			if err == io.EOF {
				break // Fin de la conexión
			} else {
				return nil, err // Error en la lectura
			}
		}
		buffer.Write(temp[:n])

		// Si ya se recibió suficiente información, romper el bucle.
		// Este criterio puede ajustarse según el protocolo
		if n < 1024 {
			break
		}
	}
	return buffer.Bytes(), nil

}

// Close cierra la conexión del socket
func (s *SocketClient) Close() error {
	return s.conn.Close()
}

package config

import (
	"net"
	"os"
)

func InitTCPListener() (net.Listener, error) {
	address := os.Getenv("TCP_ADDRESS") // Ejemplo: "localhost:9000"
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return listener, nil
}

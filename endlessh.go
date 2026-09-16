package main

import (
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"time"
)

func main() {
	var (
		addr                = flag.String("addr", "0.0.0.0:2222", "Bind to this address")
		maxConcurrentClient = flag.Uint("maxclient", 50, "Max amount of connected clients")
		delay               = flag.Uint("delay", 10, "Number of seconds to wait betweem each write")
	)
	flag.Parse()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		panic(err)
	}
	slog.Info("Listening", "addr", listener.Addr(), "maxConcurrentClient", *maxConcurrentClient, "delay", *delay)

	grp := make(chan struct{}, *maxConcurrentClient)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		select {
		case grp <- struct{}{}:
			slog.Info("New Victim", "addr", conn.RemoteAddr())
			go tarpit(conn, *delay, grp)
		default:
			slog.Warn("Server full, dropping conn", "addr", conn.RemoteAddr())
			conn.Close()
		}
	}
}

func tarpit(conn net.Conn, delay uint, grp <-chan struct{}) {
	start := time.Now()
	defer func() {
		slog.Info("Victim escaped", "addr", conn.RemoteAddr(), "duration", fmt.Sprintf("%.0f", time.Since(start).Seconds()))
		conn.Close()
		<-grp
	}()

	clientDropped := make(chan struct{})
	go func() {
		io.Copy(io.Discard, conn)
		close(clientDropped)
	}()

	for {
		_, err := fmt.Fprintf(conn, "%x\r\n", rand.Uint64())
		if err != nil {
			return
		}
		select {
		case <-time.After(time.Duration(delay) * time.Second):
		case <-clientDropped:
			return
		}
	}
}

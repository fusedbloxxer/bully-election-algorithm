package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

type MessageType string

const (
	Election MessageType = "election"
	Alive    MessageType = "OK"
	Win      MessageType = "win"
)

type AliveMessage struct {
	Content string
}

type Message struct {
	message  MessageType
	senderId int
}

// win, alive, election

type BullyNode struct {
	currentId int
	leaderId  int
	idToIpMap map[int]string
	mu        sync.Mutex
}

var node = BullyNode{}

// https://pkg.go.dev/net/rpc

// variabila care ne spune daca nodul curent a dat deja mai departe un mesaj de electie
var electionAlreadySentHigher = false

// functie apelata atunci cand un nod de rang mai mic trimite mesaj de electie catre nodul curent
func (node BullyNode) respondToSmallerNode() Message {
	var messageObj = Message{
		message:  "",
		senderId: node.currentId,
	}
	
	messageObj.message = "OK"

	// block the reading and writing of electionAlreadySentHigher
	node.mu.Lock()
	if !electionAlreadySentHigher {

		electionAlreadySentHigher = true
		node.mu.Unlock()

		// TO DO add the startElection
		// go
	}
	return messageObj

}

// functie apelata atunci cand dorim sa trimitem mesajul de electie catre un nod de rang mai mare
func startElection() Message {
	for id, ip := range node.idToIpMap {
		messageObj := Message{
			message: "",
			senderId: node.currentId,
		}

		// ignoram nodurile mai mici decat nodul curent
		if id < node.currentId {
			continue
		}

		

	}
}

func handleRequest(conn net.Conn, errorChannel chan error) {
	raw, err := bufio.NewReader(conn).ReadString('\n')

	if err != nil {
		errorChannel <- fmt.Errorf(" != could not receive message: %w", err)
		return
	}

	var message Message
	if err := json.Unmarshal([]byte(raw), &message); err != nil {
		errorChannel <- fmt.Errorf(" != could not parse message: %w", err)
		return
	}

	fmt.Println(" == Message received from ", message.senderId)

	// fmt.Println(" == Sending OK Message to ", senderId)
}

func handleReceviedMessage(recv Message) (send Message) {
	if node.currentId < recv.senderId {
		send = startElection()
	} else {
		send = respondToSmallerNode()
	}

	// // programare defensiva
	// if senderId > node.currentId {
	// 	return messageObj
	// }

	// fmt.Println(" == Sending OK Message to ", senderId)
	// messageObj.message = "OK"
}

func main() {
	// Un nod comunica atat ca si client cat si ca server
	errChannel := make(chan error, 1)
	var waitGroup sync.WaitGroup
	
	// Read configuration
	// TODO: Sa stie fiecare nod ce id are,tabela cu ip-uri/port-uri

	// Inchide conexiunea dupa ce s-a terminat rutina principala
	// Si s-au finalizat rutinele go lansate
	waitGroup.Add(2)

	// Actioneaza ca client
	go func() {
		defer waitGroup.Done()
		// TODO: Ce poate primi prin linia de comanda clientul
	}()

	// Actioneaza ca server
	go func() {
		defer waitGroup.Done()
		listener, err := net.Listen("tcp", "8080")

		if err != nil {
			errChannel <- fmt.Errorf(" != Couldn't not listen to incoming connections: %w", err)
			return
		}

		for {
			conn, err := listener.Accept()

			if err != nil {
				errChannel <- fmt.Errorf(" != Couldn't accept connection: %w", err)
				return
			}

			go handleRequest(conn, errChannel)
		}

		defer listener.Close()
	}()

	// Asteapta ca ambele fire de executie sa se incheie (client & server)
	waitGroup.Wait()
}

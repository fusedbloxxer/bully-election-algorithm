package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
)

type MessageType string

const (
	Election MessageType = "ELECTION"
	Alive    MessageType = "OK"
	Win      MessageType = "WIN"
)

type Message struct {
	Message  MessageType `json:"Message"`
	SenderId int         `json:"SenderId"`
}

type BullyNode struct {
	currentId int
	leaderId  int
	idToIpMap map[int]string
	Mu        sync.Mutex
}

var node = BullyNode{}

// variabila care ne spune daca nodul curent a dat deja mai departe un mesaj de electie
var electionAlreadySentHigher bool = false

// variabila care ne spune daca nodul curent stie ca altul mai mare este in viata
var higherNodeIsAlive = false

// functie apelata atunci cand un nod de rang mai mic trimite mesaj de electie catre nodul curent
func respondToSmallerNode(connection net.Conn) error {

	fmt.Println(" == Sending OK to: ", connection.LocalAddr())

	var messageObj = Message{
		Message:  "OK",
		SenderId: node.currentId,
	}

	messageInBytes, err := json.Marshal(&messageObj)

	if err != nil {
		return err
	}

	fmt.Fprintf(connection, "%s\n", string(messageInBytes))

	return nil

}

// functie apelata atunci cand dorim sa trimitem mesajul de electie catre un nod de rang mai mare
func startElectionToConnection(connection net.Conn) error {

	fmt.Println(" == Sending ELECTION to: ", connection.LocalAddr())

	messageObj := Message{
		Message:  "ELECTION",
		SenderId: node.currentId,
	}

	messageInBytes, err := json.Marshal(&messageObj)

	if err != nil {
		return err
	}

	fmt.Fprintf(connection, "%s\n", string(messageInBytes))
	return nil
}

// functie apelata atunci cand nodul curent vrea sa transmita ca el este noul lider
func sendCoordinatorMessage(connection net.Conn) error {

	fmt.Println(" == Sending WIN to: ", connection.LocalAddr())

	messageObj := Message{
		Message:  "WIN",
		SenderId: node.currentId,
	}

	messageInBytes, err := json.Marshal(&messageObj)

	if err != nil {
		return err
	}

	fmt.Fprintf(connection, "%s\n", string(messageInBytes))
	return nil
}

// functie apelata atunci cand nodul curent a primit un mesaj de WIN de la noul lider
func ReceiveCoordinatorMessage(message Message) {

	fmt.Println(" == Received WIN from: ", message.SenderId)

	node.leaderId = message.SenderId
}

func sendWinBroadcast(errorChannel chan error) {
	fmt.Printf(" == Sending WIN Broadcast...\n")
	node.leaderId = node.currentId
	for id, ip := range node.idToIpMap {
		if id != node.currentId {
			fmt.Printf(" == Send WIN to %v at %v\n", id, ip)
			con, err := net.Dial("tcp", ip)

			if err != nil {
				errorChannel <- fmt.Errorf(" != Could not connect to %v: %w", id, err)
				continue
			}

			if err = sendCoordinatorMessage(con); err != nil {
				errorChannel <- fmt.Errorf(" != Could not send election to %v: %w", id, err)
				continue
			}
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

	fmt.Println(" == Message received from ", message.SenderId)

	handleReceivedMessage(conn, message, errorChannel)
}

// Executa o actiune in functie de tipul mesajului pe care la primiti
// Precum si luand in considerare id-ul sender-ului
func handleReceivedMessage(
	conn net.Conn,
	recv Message,
	errorChannel chan error,
) {
	switch recv.Message {
	case Election:
		if recv.SenderId >= node.currentId {
			log.Fatalf(" != A smaller node cannot receive election messages from higher nodes")
		}
		// trimite mesaj alive nodului mai mic care incearca electia
		if err := respondToSmallerNode(conn); err != nil {
			errorChannel <- fmt.Errorf(" != Could not respond to smaller node: %v: %w", recv.SenderId, err)
		}
	case Win:
		// a fost ales un nou coordonator/lider care este marcat
		ReceiveCoordinatorMessage(recv)
	case Alive:
		// asteapta o vreme win-ul.
		// daca nu primeste, incepe el electia

		// polling
		if !higherNodeIsAlive {
			sendWinBroadcast(errorChannel)
		}
	default:
		errorChannel <- fmt.Errorf(" != Invalid message type received from %v: %s", recv.SenderId, recv.Message)
		return
	}
}

func BroadcastElection(errorChannel chan error) {
	fmt.Println(" == Sending ELECT Broadcast...")
	// verificam daca nu mai exista vreun nod mai mare in viata
	// si atunci nodul curent este cel mai mare, acesta trimite win
	node.Mu.Lock()
	if !higherNodeIsAlive {
		sendWinBroadcast(errorChannel)
		node.Mu.Unlock()
		return
	}

	// blocam citirea si scrierea variabilei electionAlreadySentHigher
	// pentru a evita o conditie de cursa
	node.Mu.Lock()
	if !electionAlreadySentHigher {
		electionAlreadySentHigher = true
		node.Mu.Unlock()
	} else {
		return
	}

	// deschide cate o conexiune cu fiecare nod mai mare si le trimite
	// un mesaj de electie
	for id, ip := range node.idToIpMap {
		if id > node.currentId {
			con, err := net.Dial("tcp", ip)

			if err != nil {
				errorChannel <- fmt.Errorf(" != Could not connect to %v: %w", id, err)
				continue
			}

			if err = startElectionToConnection(con); err != nil {
				errorChannel <- fmt.Errorf(" != Could not send election to %v: %w", id, err)
				continue
			}
		}
	}

	// ne asiguram ca variabila electionAlreadySentHigher
	// este actualizata dupa ce se trimit toate mesajele de electie
	node.Mu.Lock()
	electionAlreadySentHigher = false
	node.Mu.Unlock()

	fmt.Printf(" == Wait for ALIVE messages\n")
	now := time.Now()
	pollInterval := time.Duration(viper.GetInt("pollInterval")) * time.Millisecond
	ticker := time.NewTicker(pollInterval)

	for t := range ticker.C {
		timeElapsed := t.Sub(now)

		fmt.Printf(" == Current elapsed: %v\n", timeElapsed)

		if timeElapsed > time.Duration(viper.GetInt("timeout"))*time.Millisecond {
			fmt.Printf(" == No one answered\n")
			break
		}

		node.Mu.Lock()
		if higherNodeIsAlive {
			ticker.Stop()
			break
		}
		node.Mu.Unlock()
	}

	ticker.Stop()

	node.Mu.Lock()
	if !higherNodeIsAlive {
		sendWinBroadcast(errorChannel)
	}
	node.Mu.Unlock()
}

func StartServer(address string, waitGroup sync.WaitGroup, errChannel chan error) {
	defer waitGroup.Done()
	fmt.Println("Starting Server on address ", address)
	listener, err := net.Listen("tcp", address)

	if err != nil {
		errChannel <- fmt.Errorf(" != Couldn't not listen to incoming connections: %w", err)
		return
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()

		if err != nil {
			errChannel <- fmt.Errorf(" != Couldn't accept connection: %w", err)
			return
		}

		go handleRequest(conn, errChannel)
	}
}

func StartClient(waitGroup sync.WaitGroup, errorChannel chan error) {
	defer waitGroup.Done()
	maxNodes := viper.GetInt("maxNodes")

	visited := make(map[int]int)
	for i := 1; i <= maxNodes; i++ {
		visited[i] = 0
	}

	fmt.Println("Insert id number from 1 to ", maxNodes)
	var clientID int

	ok := 0
	for ok == 0 {
		fmt.Scanf("%d", &clientID)
		fmt.Printf("Client-Id: %d\n", clientID)
		if visited[clientID] == 1 {
			fmt.Println("This node ID is already taken!")
		} else {
			ok = 1
			visited[clientID] = 1
		}
	}

	nodeIP := "127.0.0.1:" + fmt.Sprint(viper.GetInt("ipPort")+clientID-1)
	fmt.Println("My IP is: ", nodeIP)
	address := "127.0.0.1:" + fmt.Sprint(viper.GetInt("maxNodes")+viper.GetInt("ipPort")-1)
	con, err := net.Dial("tcp", address)

	// Lanseaza server-ul
	//go StartServer(nodeIP, waitGroup, errorChannel)

	if err != nil {
		errorChannel <- fmt.Errorf(" != Could not connect to %s: %w", address, err)
	} else {
		defer con.Close()
	}

	fmt.Println("Log: Invoking Elections")
	BroadcastElection(errorChannel)
}

func SetupConfig() {
	viper.SetConfigName("config")
	viper.SetConfigType("json")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		panic(fmt.Errorf(" != Fatal error config file: %w", err))
	}

	viper.OnConfigChange(func(e fsnotify.Event) {
		fmt.Println("Config file changed:", e.Name)
	})

	viper.WatchConfig()
}

func main() {
	// Un nod comunica atat ca si client cat si ca server
	errChannel := make(chan error, 1)
	var waitGroup sync.WaitGroup

	// Inchide conexiunea dupa ce s-a terminat rutina principala
	// Si s-au finalizat rutinele go lansate
	waitGroup.Add(2)

	// Fa configuratia initiala
	SetupConfig()

	// Lanseaza clientul
	go StartClient(waitGroup, errChannel)

	// Asculta pe canalul cu erori
	for err := range errChannel {
		fmt.Println(err)
	}

	// Asteapta ca ambele fire de executie sa se incheie (client & server)
	waitGroup.Wait()
}

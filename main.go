package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/viper"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

type MessageType string

const (
	Election MessageType = "ELECTION"
	Win      MessageType = "WIN"
	Alive    MessageType = "OK"
	Ack      MessageType = "ACK"
)

type Message struct {
	Content  MessageType `json:"Content"`
	SenderId int         `json:"SenderId"`
}

type BullyNode struct {
	currentId   int
	leaderId    int
	idToAddress map[int]string
	Mu          sync.Mutex
}

var node = BullyNode{}

// variabila care ne spune daca nodul curent a dat deja mai departe un mesaj de electie
var electionAlreadySentHigher = false

// variabila care ne spune daca nodul curent stie ca altul mai mare este in viata
var higherNodeIsAlive = false

// variabila care ne spune daca a primit mesaj de win
var hasReceivedWin = false

func sendMessage(connection net.Conn, message Message) error {
	fmt.Printf(" == Sending %s to: %d\n", message.Content, message.SenderId)

	messageInBytes, err := json.Marshal(&message)

	if err != nil {
		return err
	}

	if _, err = fmt.Fprintf(connection, "%s\n", messageInBytes); err != nil {
		return fmt.Errorf(" != Could not send %s message to %d: %w", message.Content, message.SenderId, err)
	}

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
	for id, ip := range node.idToAddress {
		if id != node.currentId {
			fmt.Printf(" == Send WIN to %v at %v\n", id, ip)
			con, err := net.Dial("tcp", ip)

			if err != nil {
				errorChannel <- fmt.Errorf(" != Could not connect to %v: %w", id, err)
				continue
			}

			if err = sendMessage(con, Message{Win, id}); err != nil {
				errorChannel <- fmt.Errorf(" != Could not send election to %v: %w", id, err)
				continue
			}
		}
	}
}

func handleRequest(conn net.Conn, errorChannel chan error) {
	defer conn.Close()
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

	fmt.Println(" == Content received from ", message.SenderId)
	handleReceivedMessage(conn, message, errorChannel)
}

// Executa o actiune in functie de tipul mesajului pe care la primiti
// Precum si luand in considerare id-ul sender-ului
func handleReceivedMessage(
	conn net.Conn,
	recv Message,
	errorChannel chan error,
) {
	switch recv.Content {
	case Election:
		if recv.SenderId >= node.currentId {
			log.Fatalf(" != A smaller node cannot receive election messages from higher nodes")
		}
		// trimite mesaj alive nodului mai mic care incearca electia
		if err := sendMessage(conn, recv); err != nil {
			errorChannel <- fmt.Errorf(" != Could not respond to smaller node: %v: %w", recv.SenderId, err)
		}
	case Win:
		// a fost ales un nou coordonator/lider care este marcat
		ReceiveCoordinatorMessage(recv)
	case Alive:
		// asteapta o vreme win-ul.
		// daca nu primeste, incepe el electia
		fmt.Printf(" == Wait for WIN message\n")

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
				node.Mu.Unlock()
				break
			}
			node.Mu.Unlock()
		}

		ticker.Stop()

		// polling
		if !higherNodeIsAlive {
			sendWinBroadcast(errorChannel)
		}
	case Ack:
		fmt.Printf(" == Received ack from %d\n", recv.SenderId)
	default:
		errorChannel <- fmt.Errorf(" != Invalid message type received from %v: %s", recv.SenderId, recv.Content)
		return
	}
}

func BroadcastElection(errorChannel chan error, justLaunched bool) {
	fmt.Println(" == Sending ELECT Broadcast...")
	// verificam daca nu mai exista vreun nod mai mare in viata
	// si atunci nodul curent este cel mai mare, acesta trimite win
	node.Mu.Lock()
	if !justLaunched && !higherNodeIsAlive {
		sendWinBroadcast(errorChannel)
		node.Mu.Unlock()
		return
	}
	node.Mu.Unlock()

	// blocam citirea si scrierea variabilei electionAlreadySentHigher
	// pentru a evita o conditie de cursa
	node.Mu.Lock()
	if !electionAlreadySentHigher {
		electionAlreadySentHigher = true
		node.Mu.Unlock()
	} else {
		node.Mu.Unlock()
		return
	}

	// deschide cate o conexiune cu fiecare nod mai mare si le trimite
	// un mesaj de electie
	for id, ip := range node.idToAddress {
		if id > node.currentId {
			con, err := net.Dial("tcp", ip)

			if err != nil {
				errorChannel <- fmt.Errorf(" != Could not connect to %v: %w", id, err)
				continue
			}

			if err = sendMessage(con, Message{Election, id}); err != nil {
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
			node.Mu.Unlock()
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

func StartServer(address string, waitGroup *sync.WaitGroup, statusChannel chan bool, errChannel chan error) {
	defer waitGroup.Done()
	fmt.Println("Starting Server on address ", address)
	listener, err := net.Listen("tcp", address)

	if err != nil {
		errChannel <- fmt.Errorf(" != Could not start the server: %w", err)
		statusChannel <- false
		return
	}

	statusChannel <- true
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

func StartClient(clientServerGroup *sync.WaitGroup, errorChannel chan error) {
	defer clientServerGroup.Done()
	maxNodes := viper.GetInt("maxNodes")

	fmt.Println("Insert id number from 1 to ", maxNodes)
	var clientID int

	fmt.Scanf("%d", &clientID)
	fmt.Printf("Client-Id: %d\n", clientID)

	node.currentId = clientID
	nodeIP := "127.0.0.1:" + fmt.Sprint(viper.GetInt("ipPort")+node.currentId-1)

	// Primeste status de la server daca s-a pornit cu succes
	statusChannel := make(chan bool, 1)

	// Lanseaza server-ul
	go StartServer(nodeIP, clientServerGroup, statusChannel, errorChannel)

	// Verifica statusul serverului
	if !<-statusChannel {
		clientServerGroup.Done()
		return
	}

	fmt.Println("My IP is: ", nodeIP)
	fmt.Println("Log: Invoking Elections")
	BroadcastElection(errorChannel, true)

	for {
		fmt.Printf("Press enter for %d to communicate with coordinator %d.\n", node.currentId, node.leaderId)

		if _, err := fmt.Scanln(); err != nil {
			errorChannel <- fmt.Errorf(" != Could not get message from cmd: %w", err)
			continue
		}

		if node.leaderId == node.currentId {
			fmt.Printf(" == I am the current coordinator (%d)\n", node.currentId)
			continue
		}

		dialer := net.Dialer{Timeout: time.Duration(viper.GetInt("timeout")) * time.Millisecond}
		leaderConnection, err := dialer.Dial("tcp", node.idToAddress[node.leaderId])

		if err == nil {
			msg := Message{
				SenderId: node.currentId,

				Content: Ack,
			}

			serial, err := json.Marshal(msg)

			if err != nil {
				errorChannel <- fmt.Errorf(" != Could not serialize ACK message: %w", err)
			}

			_, err = fmt.Fprintf(leaderConnection, "%s\n", serial)

			if err != nil {
				errorChannel <- fmt.Errorf(" != Failed to send ACK message: %w", err)
			}

			_ = leaderConnection.Close()
			continue
		}

		errorChannel <- fmt.Errorf(" != Could not contact the coordinator (%d): %w", node.leaderId, err)
		BroadcastElection(errorChannel, false)
	}
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

	// Initializeaza mapa id-address
	port := viper.GetInt("ipPort")
	node.idToAddress = make(map[int]string)
	for i := 1; i != viper.GetInt("maxNodes")+1; i++ {
		node.idToAddress[i] = fmt.Sprintf("127.0.0.1:%d", port+i-1)
	}
}

func main() {
	// Afiseaza PID-ul local
	fmt.Println("PID: ", os.Getpid())

	// Un nod comunica atat ca si client cat si ca server
	errChannel := make(chan error, 1)
	var clientServerGroup sync.WaitGroup

	// Inchide conexiunea dupa ce s-a terminat rutina principala
	// Si s-au finalizat rutinele go lansate
	clientServerGroup.Add(2)

	// Fa configuratia initiala
	SetupConfig()

	// Lanseaza clientul
	go StartClient(&clientServerGroup, errChannel)

	// Asculta pe canalul cu erori
	for err := range errChannel {
		fmt.Println(err)
	}

	// Asteapta ca ambele fire de executie sa se incheie (client & server)
	clientServerGroup.Wait()
}

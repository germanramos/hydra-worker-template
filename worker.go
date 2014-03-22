package worker

import (
	// zmq "github.com/alecthomas/gozmq"
	zmq "github.com/innotech/hydra/vendors/github.com/alecthomas/gozmq"
	uuid "github.com/innotech/hydra/vendors/github.com/nu7hatch/gouuid"
)

const (
	//  This is the version of MDP/Client we implement
	// MDPC_CLIENT = "MDPC01"

	//  This is the version of MDP/Worker we implement
	MDPW_WORKER = "MDPW01"

	// MDP/Server commands, as strings
	// TODO: delete MDPW
	SIGNAL_READY      = "\001"
	SIGNAL_REQUEST    = "\002"
	SIGNAL_REPLY      = "\003"
	SIGNAL_HEARTBEAT  = "\004"
	SIGNAL_DISCONNECT = "\005"

	// TODO: from zhelpers
	HEARTBEAT_LIVENESS = 3 //  3-5 is reasonable
)

// var Commands = []string{"", "READY", "REQUEST", "REPLY", "HEARTBEAT", "DISCONNECT"}

type Worker interface {
	Close()
	Recv([][]byte) [][]bytess
	// Added
	Run()
}

type mdWorker struct {
	// TODO: Hydra server
	broker  string // broker URI
	context *zmq.Context
	service string
	verbose bool
	worker  *zmq.Socket

	heartbeat   time.Duration
	heartbeatAt time.Time
	liveness    int
	reconnect   time.Duration

	expectReply bool
	replyTo     []byte
}

func NewWorker(broker, service string, verbose bool) Worker {
	context, _ := zmq.NewContext()
	self := &mdWorker{
		broker:    broker,
		context:   context,
		service:   service,
		verbose:   verbose,
		heartbeat: 2500 * time.Millisecond,
		liveness:  0,
		reconnect: 2500 * time.Millisecond,
	}
	self.reconnectToBroker()
	return self
}

func (self *mdWorker) reconnectToBroker() {
	if self.worker != nil {
		self.worker.Close()
	}
	self.worker, _ = self.context.NewSocket(zmq.DEALER)
	// Pending messages shall be discarded immediately when the socket is closed with Close()
	self.worker.SetLinger(0)
	self.worker.Connect(self.broker)
	if self.verbose {
		log.Printf("Connecting to broker at %s...\n", self.broker)
	}
	self.sendToBroker(MDPW_READY, []byte(self.service), nil)
	self.liveness = HEARTBEAT_LIVENESS
	self.heartbeatAt = time.Now().Add(self.heartbeat)
}

func (self *mdWorker) sendToBroker(command string, option []byte, msg [][]byte) {
	if len(option) > 0 {
		msg = append([][]byte{option}, msg...)
	}

	msg = append([][]byte{nil, []byte(MDPW_WORKER), []byte(command)}, msg...)
	if self.verbose {
		log.Printf("I: sending %X to broker\n", command)
		Dump(msg)
	}
	self.worker.SendMultipart(msg, 0)
}

func (self *mdWorker) Close() {
	if self.worker != nil {
		self.worker.Close()
	}
	self.context.Close()
}

func (self *mdWorker) Recv(reply [][]byte) (msg [][]byte) {
	//  Format and send the reply if we were provided one

	if len(reply) == 0 && self.expectReply {
		panic("Error reply")
	}

	if len(reply) > 0 {
		if len(self.replyTo) == 0 {
			panic("Error replyTo")
		}
		reply = append([][]byte{self.replyTo, nil}, reply...)
		self.sendToBroker(MDPW_REPLY, nil, reply)
	}

	self.expectReply = true

	for {
		items := zmq.PollItems{
			zmq.PollItem{Socket: self.worker, Events: zmq.POLLIN},
		}

		_, err := zmq.Poll(items, self.heartbeat)
		if err != nil {
			panic(err) //  Interrupted
		}

		if item := items[0]; item.REvents&zmq.POLLIN != 0 {
			msg, _ = self.worker.RecvMultipart(0)
			if self.verbose {
				log.Println("I: received message from broker: ")
				Dump(msg)
			}
			self.liveness = HEARTBEAT_LIVENESS
			if len(msg) < 3 {
				panic("Invalid msg") //  Interrupted
			}

			header := msg[1]
			if string(header) != MDPW_WORKER {
				panic("Invalid header") //  Interrupted
			}

			switch command := string(msg[2]); command {
			case MDPW_REQUEST:
				//  We should pop and save as many addresses as there are
				//  up to a null part, but for now, just save one...
				self.replyTo = msg[3]
				msg = msg[5:]
				return
			case MDPW_HEARTBEAT:
				// do nothin
			case MDPW_DISCONNECT:
				self.reconnectToBroker()
			default:
				log.Println("E: invalid input message:")
				Dump(msg)
			}
		} else if self.liveness--; self.liveness <= 0 {
			if self.verbose {
				log.Println("W: disconnected from broker - retrying...")
			}
			time.Sleep(self.reconnect)
			self.reconnectToBroker()
		}

		//  Send HEARTBEAT if it's time
		if self.heartbeatAt.Before(time.Now()) {
			self.sendToBroker(MDPW_HEARTBEAT, nil, nil)
			self.heartbeatAt = time.Now().Add(self.heartbeat)
		}
	}

	return
}

// type Worker interface {
// 	Close()
// 	Run()
// }

// type HydraWorker struct{}

// func (w *Worker) Connect() {
// 	// Tell the hydra server we're ready for work
// 	dealer.SendMultipart([][]byte{[]byte(""), []byte("connecting")}, 0)
// 	for {
// 		// Get workload from broker
// 		parts, _ := dealer.RecvMultipart(0)
// 		result := f(parts[4])
// 		worker.SendMultipart([][]byte{identity, empty, []byte("OK")}, 0)
// 	}
// }

// func (w *Worker) Run(f func([]byte) []byte) {
// 	context, _ := zmq.NewContext()
// 	defer context.Close()

// 	// The DEALER socket gives us the reply envelope and message
// 	dealer, _ := context.NewSocket(zmq.DEALER)
// 	defer dealer.Close()
// 	dealer.SetIdentity(randomString())
// 	// TODO: Config protocol
// 	// TODO: Config address
// 	dealer.Connect("tcp://*:5671")

// 	for {

// 		// Get workload from broker
// 		parts, _ := dealer.RecvMultipart(0)
// 		result := f(parts[4])
// 		worker.SendMultipart([][]byte{identity, empty, []byte("OK")}, 0)
// 	}
// }

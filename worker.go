package worker

import (
	zmq "github.com/innotech/hydra/vendors/github.com/alecthomas/gozmq"
	uuid "github.com/innotech/hydra/vendors/github.com/nu7hatch/gouuid"
	"log"
	"time"
)

const (
	SIGNAL_READY      = "\001"
	SIGNAL_REQUEST    = "\002"
	SIGNAL_REPLY      = "\003"
	SIGNAL_HEARTBEAT  = "\004"
	SIGNAL_DISCONNECT = "\005"

	HEARTBEAT_LIVENESS = 3
)

type Worker interface {
	Close()
	Recv([][]byte) [][]bytess
}

type lbWorker struct {
	broker  string // Hydra Load Balancer address
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
	self := &lbWorker{
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

func (self *lbWorker) reconnectToBroker() {
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
	self.sendToBroker(SIGNAL_READY, []byte(self.service), nil)
	self.liveness = HEARTBEAT_LIVENESS
	self.heartbeatAt = time.Now().Add(self.heartbeat)
}

func (self *lbWorker) sendToBroker(command string, option []byte, msg [][]byte) {
	if len(option) > 0 {
		msg = append([][]byte{option}, msg...)
	}

	msg = append([][]byte{nil, []byte(SIGNAL_WORKER), []byte(command)}, msg...)
	if self.verbose {
		log.Printf("Sending %X to broker\n", command)
		Dump(msg)
	}
	self.worker.SendMultipart(msg, 0)
}

func (self *lbWorker) Close() {
	if self.worker != nil {
		self.worker.Close()
	}
	self.context.Close()
}

func (self *lbWorker) Recv(reply [][]byte) (msg [][]byte) {
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
				log.Println("Received message from broker: ")
				Dump(msg)
			}
			self.liveness = HEARTBEAT_LIVENESS
			if len(msg) < 3 {
				panic("Invalid msg") //  Interrupted
			}

			header := msg[1]
			if string(header) != SIGNAL_WORKER {
				panic("Invalid header") //  Interrupted
			}

			switch command := string(msg[2]); command {
			case SIGNAL_REQUEST:
				//  We should pop and save as many addresses as there are
				//  up to a null part, but for now, just save one...
				self.replyTo = msg[3]
				msg = msg[5:]
				return
			case SIGNAL_HEARTBEAT:
				// do nothin
			case SIGNAL_DISCONNECT:
				self.reconnectToBroker()
			default:
				log.Println("Invalid input message:")
				Dump(msg)
			}
		} else if self.liveness--; self.liveness <= 0 {
			if self.verbose {
				log.Println("Disconnected from broker - retrying...")
			}
			time.Sleep(self.reconnect)
			self.reconnectToBroker()
		}

		//  Send HEARTBEAT if it's time
		if self.heartbeatAt.Before(time.Now()) {
			self.sendToBroker(SIGNAL_HEARTBEAT, nil, nil)
			self.heartbeatAt = time.Now().Add(self.heartbeat)
		}
	}

	return
}

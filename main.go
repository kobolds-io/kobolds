// Copyright 2018 The Mangos Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use file except in compliance with the License.
// You may obtain a copy of the license at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// reqprep implements a request/reply example.  node0 is a listening
// rep socket, and node1 is a dialing req socket.
//
// To use:
//
//	$ go build .
//	$ url=tcp://127.0.0.1:40899
//	$ ./reqrep node0 $url & node0=$! && sleep 1
//	$ ./reqrep node1 $url
//	$ kill $node0
package main

import (
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/fxamacker/cbor/v2"
	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"

	// // register transports
	_ "go.nanomsg.org/mangos/v3/transport/all"
)

func die(format string, v ...interface{}) {
	fmt.Fprintln(os.Stderr, fmt.Sprintf(format, v...))
	os.Exit(1)
}

type Message struct {
	Id            string `cbor:"id"`
	SenderId      string `cbor:"sender_id"`
	ReceiverId    string `cbor:"receiver_id"`
	TransactionId string `cbor:"transaction_id"`
}

type AuthMessage struct {
	Token string `cbor:"token"`
}

func node0(url string) {
	var sock mangos.Socket
	var err error

	if sock, err = rep.NewSocket(); err != nil {
		die("can't get new rep socket: %s", err)
	}
	if err = sock.Listen(url); err != nil {
		die("can't listen on rep socket: %s", err.Error())
	}

	rawAuthMsg, err := sock.Recv()
	if err != nil {
		die("cant receive message: %s", err.Error())
	}

	var authMsg AuthMessage
	err = cbor.Unmarshal(rawAuthMsg, &authMsg)
	if err != nil {
		die("cant unmarshal auth message: %s", err.Error())
	}

	fmt.Printf("auth message %#v\n", authMsg)

	for {

		// Could also use sock.RecvMsg to get header
		msg, err := sock.Recv()
		if err != nil {
			die("cannot receive on rep socket: %s", err.Error())
		}

		var request Message
		err = cbor.Unmarshal(msg, &request)
		if err != nil {
			slog.Error("error unmarshalling request", "error", err)
			continue
		}

		response := Message{
			Id:            "some id",
			SenderId:      "some sender id",
			ReceiverId:    "some receiver id",
			TransactionId: "some transaction id",
		}

		b, err := cbor.Marshal(&response)
		if err != nil {
			slog.Error("unable to marshal message", "error", err)
			return
		}

		err = sock.Send([]byte(b))
		if err != nil {
			die("can't send reply: %s", err.Error())
		}

	}
}

func node1(url string) {
	var sock mangos.Socket
	var err error
	var msg []byte

	if sock, err = req.NewSocket(); err != nil {
		die("can't get new req socket: %s", err.Error())
	}
	if err = sock.Dial(url); err != nil {
		die("can't dial on req socket: %s", err.Error())
	}

	data, err := cbor.Marshal(&AuthMessage{
		Token: "some cool auth token",
	})
	if err != nil {
		die("can't marshal auth message: %s", err.Error())
	}

	if err = sock.Send(data); err != nil {
		die("cant authenticate with server")
	}

	totalStart := time.Now()
	rtts := []time.Duration{}
	requests := 0
	responses := 0

	for i := 0; i < 100_000; i++ {
		start := time.Now()
		request := Message{
			Id:            "some bytes to fill up here",
			SenderId:      "some bytes to fill up here",
			ReceiverId:    "some bytes to fill up here",
			TransactionId: "some bytes to fill up here",
		}

		data, err := cbor.Marshal(&request)
		if err != nil {
			slog.Error("error marshalling message", "error", err)
			continue
		}

		requests++

		// fmt.Printf("NODE1: SENDING DATE REQUEST %s\n", "DATE")
		if err = sock.Send(data); err != nil {
			die("can't send message on push socket: %s", err.Error())
		}
		if msg, err = sock.Recv(); err != nil {
			die("can't receive date: %s", err.Error())
		}

		responses++
		rtts = append(rtts, time.Since(start))
	}

	var parsedMsg Message
	err = cbor.Unmarshal(msg, &parsedMsg)
	if err != nil {
		slog.Error("unable to parse message")
	}

	fmt.Printf("parsed message %#v\n", parsedMsg)

	var sum int64
	var mininum int64
	var maximum int64
	for _, dur := range rtts {
		d := int64(dur)
		if d > maximum {
			maximum = d
		}
		if mininum == 0 {
			mininum = d
		} else if mininum > d {
			mininum = d
		}

		sum += d

	}

	avg := float64(sum) / float64(len(rtts))
	fmt.Println("total roundtrips", len(rtts))
	fmt.Println("total requests sent", requests)
	fmt.Println("total replies received", responses)
	fmt.Println("total messages sent/received", responses+requests)
	fmt.Println("min iter time", float64(mininum)/float64(time.Microsecond), "microseconds")
	fmt.Println("max iter time", float64(maximum)/float64(time.Microsecond), "microseconds")
	fmt.Println("average iter time", avg/float64(time.Microsecond), "microseconds")
	fmt.Println("total command time", time.Since(totalStart))
	fmt.Println("max msgs per second", float64(time.Second)/float64(mininum))
	fmt.Println("min msgs per second", float64(time.Second)/float64(maximum))
	fmt.Println("avg msgs per second", float64(time.Second)/avg)
	sock.Close()
}

func main() {
	if len(os.Args) > 2 && os.Args[1] == "node0" {
		node0(os.Args[2])
		os.Exit(0)
	}
	if len(os.Args) > 2 && os.Args[1] == "node1" {
		node1(os.Args[2])
		os.Exit(0)
	}
	fmt.Fprintf(os.Stderr, "Usage: reqrep node0|node1 <URL>\n")
	os.Exit(1)
}

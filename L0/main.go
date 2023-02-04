package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/gofiber/fiber/v2"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/nats-io/nats.go"
)

const dbTableName = "orders"

var orders sync.Map

type order struct {
	Id   string `db:"id"`
	Data []byte `db:"data"`
}

type orderData struct {
	OrderUID    string `json:"order_uid"`
	TrackNumber string `json:"track_number"`
}

func updateDB(ord ...order) error {
	db, err := sqlx.Connect("postgres", "user=mkdr dbname=wb_l0 sslmode=disable")
	if err != nil {
		return errors.New("error connect to database")
	}

	if len(ord) != 0 {
		db.NamedExec(fmt.Sprintf("INSERT INTO %s (id, data) VALUES (:id, :data)", dbTableName), order{Id: ord[0].Id, Data: ord[0].Data})
	}

	list := []order{}
	if err = db.Select(&list, fmt.Sprintf("SELECT * FROM %s", dbTableName)); err != nil {
		return errors.New("error read database")
	}

	for _, order := range list {
		orders.Store(order.Id, string(order.Data))
	}

	return nil
}

func natsRecv(nc *nats.Conn) {
	ec, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)

	recv := make(chan *order)
	ec.BindRecvChan("data", recv)

	for {
		ord := <-recv
		updateDB(order{Id: ord.Id, Data: ord.Data})
	}
}

func launchNats() chan *order {
	nc, _ := nats.Connect(nats.DefaultURL)
	go natsRecv(nc)

	ec, _ := nats.NewEncodedConn(nc, nats.JSON_ENCODER)

	send := make(chan *order)
	ec.BindSendChan("data", send)

	return send
}

func getOrder(ctx *fiber.Ctx) error {
	v, ok := orders.Load(ctx.Query("id"))
	if ok {
		return ctx.SendString(fmt.Sprintf("%s", v))
	}

	return ctx.SendString("Not found order")
}

func addOrder(ctx *fiber.Ctx, natsSend chan *order) error {
	var data orderData

	body := ctx.Body()
	json.Unmarshal(body, &data)

	if data.OrderUID != "" && data.TrackNumber != "" {
		natsSend <- &order{Id: data.OrderUID, Data: body}
		return ctx.SendString("Success add order")
	}

	return ctx.SendString("Error add order")
}

func main() {
	if err := updateDB(); err != nil {
		log.Fatal(err)
	}

	ncSend := launchNats()

	app := fiber.New()

	app.Static("home", "home.html")

	app.Get("order", getOrder)

	app.Post("order", func(ctx *fiber.Ctx) error {
		return addOrder(ctx, ncSend)
	})

	app.Listen(":8080")
}

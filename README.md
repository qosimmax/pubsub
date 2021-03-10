# pubsub
package main

```
import (
	"context"
	"fmt"

	"github.com/qosimmax/pubsub/events"
	"github.com/qosimmax/pubsub/pkg/kafka"
)

type Order struct {
}
type Customer struct {
}

func (o *Order) create(ctx context.Context, payload []byte) ([]byte, error) {
	fmt.Println("ORDER", string(payload))
	return nil, nil
}

func (c *Customer) create(ctx context.Context, payload []byte) ([]byte, error) {
	fmt.Println("CUSTOMER", string(payload))
	return []byte(`{"success":true}`), nil
}

func main() {

	var (
		orderHandlers    Order
		customerHandlers Customer
	)

	kafkaConfig := kafka.Config{
		Address: "localhost:9092",
		GroupId: "group-id",
	}

	router := events.NewRouter(kafka.NewKafkaSubscribe(&kafkaConfig), kafka.NewKafkaPublisher(&kafkaConfig))
	router.AddNoPublisherHandler("v1.events.order", orderHandlers.create)
	router.AddHandler("v1.events.customer", "v1.events.customer.response", customerHandlers.create)
	_ = router.Run()
}
```


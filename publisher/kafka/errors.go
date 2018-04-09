package kafka

type ConnectionError struct {
	Reason string
}

func (c ConnectionError) Error() string {
	if c.Reason != "" {
		return c.Reason
	}
	return "failed to connect to kafka"
}


type PublishingFailed struct {
	Reason string
}

func (c PublishingFailed) Error() string {
	return "failed to publish to the kafka master - " + c.Reason
}


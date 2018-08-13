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

type SubscribeFailure struct {
	Reason string
}

func (c SubscribeFailure) Error() string {
	return "failed to subscribe to the given channel - " + c.Reason
}

type UnsubscribeFailure struct {
	Reason string
}

func (c UnsubscribeFailure) Error() string {
	return "failed to unsubscribe from given channel - " + c.Reason
}
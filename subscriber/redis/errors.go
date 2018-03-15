package redis

type ConnectionError struct {
	Reason string
}

func (c ConnectionError) Error() string {
	if c.Reason != "" {
		return c.Reason
	}
	return "failed to connect to sentinel"
}

type CouldNotGetMaster struct {
	Reason string
}

func (c CouldNotGetMaster) Error() string {
	return "could not get a redis master connection from the sentinel - " + c.Reason
}

type SubscribeFailure struct {
	Reason string
}

func (c SubscribeFailure) Error() string {
	return "failed to subscribe to the given channel - " + c.Reason
}

type ReceiveFailure struct {
	Reason string
}

func (c ReceiveFailure) Error() string {
	return "error occurred while trying to receive a message from the channel - " + c.Reason
}

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

type PublishingFailed struct {
	Reason string
}

func (c PublishingFailed) Error() string {
	return "failed to publish to the redis master - " + c.Reason
}

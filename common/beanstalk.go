package common

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"os"
	"time"
)

func DialBeanstalk() (*beanstalk.Conn, error) {
	host, b := os.LookupEnv("BEANSTALK_HOST")
	if !b {
		return nil, errors.New("fetch beanstalk host failed")
	}

	conn, err := beanstalk.DialTimeout("tcp", host, time.Second*5)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

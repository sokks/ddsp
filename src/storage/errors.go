package storage

import (
	"errors"
)

var (
	ErrQuorumNotReached = errors.New("Quorum not reached")
	ErrNotEnoughDaemons = errors.New("Not Enough Daemons Available")
	ErrUnknownDaemon    = errors.New("Unknown Daemon")
	ErrRecordNotFound   = errors.New("Record Not Found")
	ErrRecordExists     = errors.New("Already have record")

	ErrUnknownStatus = errors.New("Error Unknown")
)

type StatusCode int32

const (
	StatusOk StatusCode = iota
	StatusQuorumNotReached
	StatusNotEnoughDaemons
	StatusUnknownDaemon
	StatusRecordNotFound
	StatusRecordExists

	StatusUnknown
)

func (s StatusCode) ToError() error {
	switch s {
	case StatusOk:
		return nil
	case StatusQuorumNotReached:
		return ErrQuorumNotReached
	case StatusNotEnoughDaemons:
		return ErrNotEnoughDaemons
	case StatusUnknownDaemon:
		return ErrUnknownDaemon
	case StatusRecordNotFound:
		return ErrRecordNotFound
	case StatusRecordExists:
		return ErrRecordExists
	default:
		return ErrUnknownStatus
	}
}

func ErrToStatus(err error) StatusCode {
	if err == nil {
		return StatusOk
	}

	switch err {
	case ErrQuorumNotReached:
		return StatusQuorumNotReached
	case ErrNotEnoughDaemons:
		return StatusNotEnoughDaemons
	case ErrUnknownDaemon:
		return StatusUnknownDaemon
	case ErrRecordNotFound:
		return StatusRecordNotFound
	case ErrRecordExists:
		return StatusRecordExists
	default:
		return StatusUnknown
	}
}

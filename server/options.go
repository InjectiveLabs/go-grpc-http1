package server

import "github.com/coder/websocket"

type options struct {
	preferGRPCWeb                 bool
	webSocketCompressionMode      websocket.CompressionMode
	webSocketCompressionThreshold int
}

// Option is an object that controls the behavior of the downgrading gRPC server.
type Option interface {
	apply(o *options)
}

type optionFunc func(o *options)

func (f optionFunc) apply(o *options) {
	f(o)
}

// PreferGRPCWeb instructs the server to always send a downgraded gRPC-Web response
// if the client indicates it accepts gRPC-Web responses.
func PreferGRPCWeb(prefer bool) Option {
	return optionFunc(func(o *options) {
		o.preferGRPCWeb = prefer
	})
}

func WebSocketCompression(mode websocket.CompressionMode, threshold int) Option {
	return webSocketCompressionOption{mode: mode, threshold: threshold}
}

type webSocketCompressionOption struct {
	mode      websocket.CompressionMode
	threshold int
}

func (o webSocketCompressionOption) apply(opts *options) {
	opts.webSocketCompressionMode = o.mode
	opts.webSocketCompressionThreshold = o.threshold
}

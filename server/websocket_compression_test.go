package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"golang.stackrox.io/grpc-http1/internal/grpcproto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type compressionEchoServer struct {
	done chan struct{}
}

func (s *compressionEchoServer) handle(_ any, stream grpc.ServerStream) error {
	defer close(s.done)
	for {
		message := new(wrapperspb.BytesValue)
		err := stream.RecvMsg(message)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if bytes.Equal(message.Value, []byte("fail")) {
			return status.Error(codes.InvalidArgument, "invalid request")
		}
		if err := stream.SendMsg(message); err != nil {
			return err
		}
	}
}

type compressionTestConnection struct {
	conn     *websocket.Conn
	response *http.Response
	httpSrv  *httptest.Server
	grpcSrv  *grpc.Server
	echo     *compressionEchoServer
}

func (c *compressionTestConnection) close() {
	_ = c.conn.CloseNow()
	c.httpSrv.Close()
	c.grpcSrv.Stop()
}

func newCompressionTestConnection(ctx context.Context, t *testing.T, offered bool, opts ...Option) *compressionTestConnection {
	t.Helper()
	echo := &compressionEchoServer{done: make(chan struct{})}
	grpcSrv := grpc.NewServer(grpc.UnknownServiceHandler(echo.handle))
	t.Cleanup(grpcSrv.Stop)
	httpSrv := httptest.NewServer(CreateDowngradingHandler(grpcSrv, http.NotFoundHandler(), opts...))
	t.Cleanup(httpSrv.Close)
	mode := websocket.CompressionDisabled
	if offered {
		mode = websocket.CompressionNoContextTakeover
	}
	conn, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpSrv.URL, "http")+"/test.Service/Echo", &websocket.DialOptions{
		HTTPHeader:           http.Header{"Content-Type": {"application/grpc"}},
		Subprotocols:         []string{"grpc-ws"},
		CompressionMode:      mode,
		CompressionThreshold: 512,
	})
	require.NoError(t, err)
	fixture := &compressionTestConnection{conn: conn, response: response, httpSrv: httpSrv, grpcSrv: grpcSrv, echo: echo}
	t.Cleanup(fixture.close)
	return fixture
}

func sendCompressionMessage(ctx context.Context, t *testing.T, conn *websocket.Conn, value []byte) {
	t.Helper()
	payload, err := proto.Marshal(&wrapperspb.BytesValue{Value: value})
	require.NoError(t, err)
	frame := append(grpcproto.MakeMessageHeader(0, uint32(len(payload))), payload...)
	require.NoError(t, conn.Write(ctx, websocket.MessageBinary, frame))
}

func receiveCompressionMessage(ctx context.Context, t *testing.T, conn *websocket.Conn) []byte {
	t.Helper()
	for {
		kind, frame, err := conn.Read(ctx)
		require.NoError(t, err)
		require.Equal(t, websocket.MessageBinary, kind)
		require.NoError(t, grpcproto.ValidateGRPCFrame(frame))
		if !grpcproto.IsDataFrame(frame) {
			continue
		}
		require.Zero(t, frame[0])
		var message wrapperspb.BytesValue
		require.NoError(t, proto.Unmarshal(frame[5:], &message))
		return message.Value
	}
}

func assertCompressionClose(ctx context.Context, t *testing.T, conn *websocket.Conn, expected websocket.StatusCode) {
	t.Helper()
	for {
		_, _, err := conn.Read(ctx)
		if err != nil {
			require.Equal(t, expected, websocket.CloseStatus(err))
			return
		}
	}
}

func assertWebSocketCompression(t *testing.T, offered, compressed bool, opts ...Option) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fixture := newCompressionTestConnection(ctx, t, offered, opts...)
	extension := fixture.response.Header.Get("Sec-WebSocket-Extensions")
	if compressed {
		require.Contains(t, extension, "permessage-deflate")
		require.Contains(t, extension, "server_no_context_takeover")
		require.Contains(t, extension, "client_no_context_takeover")
	} else {
		require.Empty(t, extension)
	}
	small := []byte("ping")
	sendCompressionMessage(ctx, t, fixture.conn, small)
	require.Equal(t, small, receiveCompressionMessage(ctx, t, fixture.conn))
	large := bytes.Repeat([]byte("quote"), 512)
	for range 2 {
		sendCompressionMessage(ctx, t, fixture.conn, large)
		require.Equal(t, large, receiveCompressionMessage(ctx, t, fixture.conn))
	}
	require.NoError(t, fixture.conn.Write(ctx, websocket.MessageBinary, grpcproto.EndStreamHeader))
	assertCompressionClose(ctx, t, fixture.conn, websocket.StatusNormalClosure)
	waitFor(t, fixture.echo.done, "echo stream did not terminate after half-close")
}

func TestWebSocketCompressionDisabledByDefault(t *testing.T) {
	assertWebSocketCompression(t, true, false)
}

func TestWebSocketCompressionNegotiated(t *testing.T) {
	assertWebSocketCompression(t, true, true, WebSocketCompression(websocket.CompressionNoContextTakeover, 512))
}

func TestWebSocketCompressionRequiresClientOffer(t *testing.T) {
	assertWebSocketCompression(t, false, false, WebSocketCompression(websocket.CompressionNoContextTakeover, 512))
}

func TestWebSocketCompressionExplicitlyDisabled(t *testing.T) {
	assertWebSocketCompression(t, true, false, WebSocketCompression(websocket.CompressionDisabled, 512))
}

func TestWebSocketCompressionPreservesErrorCloseStatus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fixture := newCompressionTestConnection(ctx, t, true, WebSocketCompression(websocket.CompressionNoContextTakeover, 512))
	sendCompressionMessage(ctx, t, fixture.conn, []byte("fail"))
	assertCompressionClose(ctx, t, fixture.conn, websocket.StatusPolicyViolation)
	waitFor(t, fixture.echo.done, "failed stream did not terminate")
}

func TestWebSocketCompressionCancelsOnDisconnect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fixture := newCompressionTestConnection(ctx, t, true, WebSocketCompression(websocket.CompressionNoContextTakeover, 512))
	large := bytes.Repeat([]byte("quote"), 512)
	sendCompressionMessage(ctx, t, fixture.conn, large)
	require.Equal(t, large, receiveCompressionMessage(ctx, t, fixture.conn))
	require.NoError(t, fixture.conn.CloseNow())
	waitFor(t, fixture.echo.done, "compressed stream did not terminate after disconnect")
}

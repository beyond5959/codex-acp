package codex

import (
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestHandleNotification_FileChangePatchKindCompatibility(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		params string
		want   TurnEventType
	}{
		{
			name:   "started supports object kind",
			method: notificationItemStarted,
			params: `{"threadId":"thread-1","turnId":"turn-1","item":{"id":"item-1","type":"fileChange","changes":[{"diff":"@@ -0,0 +1 @@\n+hello\n","kind":{"type":"add"},"path":"README.md"}]}}`,
			want:   TurnEventTypeItemStarted,
		},
		{
			name:   "completed supports legacy string kind",
			method: notificationItemCompleted,
			params: `{"threadId":"thread-1","turnId":"turn-1","item":{"id":"item-1","type":"fileChange","changes":[{"diff":"@@ -1 +1 @@\n-old\n+new\n","kind":"update","path":"README.md"}]}}`,
			want:   TurnEventTypeItemCompleted,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := &Client{
				logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
				approvals:   make(map[string]pendingApproval),
				turnStreams: make(map[string]*turnStream),
				queuedTurns: make(map[string][]TurnEvent),
			}

			client.handleNotification(RPCMessage{
				JSONRPC: "2.0",
				Method:  tc.method,
				Params:  json.RawMessage(tc.params),
			})

			queued := client.queuedTurns["turn-1"]
			if len(queued) != 1 {
				t.Fatalf("queued events=%d, want 1", len(queued))
			}
			event := queued[0]
			if event.Type != tc.want {
				t.Fatalf("event type=%q, want %q", event.Type, tc.want)
			}
			if event.ItemID != "item-1" {
				t.Fatalf("item id=%q, want %q", event.ItemID, "item-1")
			}
			if event.ItemType != "fileChange" {
				t.Fatalf("item type=%q, want %q", event.ItemType, "fileChange")
			}
		})
	}
}

func TestHandleNotification_TurnCompletedIncludesErrorMessage(t *testing.T) {
	t.Parallel()

	client := &Client{
		logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		approvals:   make(map[string]pendingApproval),
		turnStreams: make(map[string]*turnStream),
		queuedTurns: make(map[string][]TurnEvent),
	}

	client.handleNotification(RPCMessage{
		JSONRPC: "2.0",
		Method:  notificationTurnCompleted,
		Params: json.RawMessage(
			`{"threadId":"thread-1","turn":{"id":"turn-1","status":"failed","error":{"message":"apply_patch verification failed","additionalDetails":"patch no longer matches current file contents","codexErrorInfo":"other"}}}`,
		),
	})

	queued := client.queuedTurns["turn-1"]
	if len(queued) != 1 {
		t.Fatalf("queued events=%d, want 1", len(queued))
	}
	event := queued[0]
	if event.Type != TurnEventTypeCompleted {
		t.Fatalf("event type=%q, want %q", event.Type, TurnEventTypeCompleted)
	}
	if event.StopReason != "error" {
		t.Fatalf("stop reason=%q, want %q", event.StopReason, "error")
	}
	if got, want := event.Message, "apply_patch verification failed: patch no longer matches current file contents [codexErrorInfo=other]"; got != want {
		t.Fatalf("message=%q, want %q", got, want)
	}
}

func TestHandleNotification_ErrorNotificationRetrying(t *testing.T) {
	t.Parallel()

	client := &Client{
		logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		approvals:   make(map[string]pendingApproval),
		turnStreams: make(map[string]*turnStream),
		queuedTurns: make(map[string][]TurnEvent),
	}

	client.handleNotification(RPCMessage{
		JSONRPC: "2.0",
		Method:  notificationError,
		Params: json.RawMessage(
			`{"threadId":"thread-1","turnId":"turn-1","willRetry":true,"error":{"message":"temporary upstream connection drop","codexErrorInfo":{"responseStreamDisconnected":{"httpStatusCode":502}}}}`,
		),
	})

	queued := client.queuedTurns["turn-1"]
	if len(queued) != 1 {
		t.Fatalf("queued events=%d, want 1", len(queued))
	}
	event := queued[0]
	if event.Type != TurnEventTypeBackendError {
		t.Fatalf("event type=%q, want %q", event.Type, TurnEventTypeBackendError)
	}
	if !event.WillRetry {
		t.Fatalf("willRetry=%t, want true", event.WillRetry)
	}
	if got, want := event.Message, "temporary upstream connection drop [codexErrorInfo=responseStreamDisconnected(httpStatusCode=502)]"; got != want {
		t.Fatalf("message=%q, want %q", got, want)
	}
}

func TestHandleNotification_ThreadTokenUsageUpdated(t *testing.T) {
	t.Parallel()

	client := &Client{
		logger:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
		approvals:   make(map[string]pendingApproval),
		turnStreams: make(map[string]*turnStream),
		queuedTurns: make(map[string][]TurnEvent),
	}

	client.handleNotification(RPCMessage{
		JSONRPC: "2.0",
		Method:  notificationThreadTokenUsageUpdated,
		Params: json.RawMessage(
			`{"threadId":"thread-1","turnId":"turn-1","tokenUsage":{"last":{"cachedInputTokens":1000,"inputTokens":4000,"outputTokens":500,"reasoningOutputTokens":250,"totalTokens":5750},"modelContextWindow":200000,"total":{"cachedInputTokens":5000,"inputTokens":35000,"outputTokens":12000,"reasoningOutputTokens":1000,"totalTokens":53000}}}`,
		),
	})

	queued := client.queuedTurns["turn-1"]
	if len(queued) != 1 {
		t.Fatalf("queued events=%d, want 1", len(queued))
	}
	event := queued[0]
	if event.Type != TurnEventTypeTokenUsageUpdated {
		t.Fatalf("event type=%q, want %q", event.Type, TurnEventTypeTokenUsageUpdated)
	}
	if event.TokenUsage == nil {
		t.Fatalf("token usage event missing token usage payload")
	}
	if got, want := event.TokenUsage.Total.TotalTokens, int64(53000); got != want {
		t.Fatalf("total.totalTokens=%d, want %d", got, want)
	}
	if event.TokenUsage.ModelContextWindow == nil || *event.TokenUsage.ModelContextWindow != 200000 {
		t.Fatalf("modelContextWindow=%v, want 200000", event.TokenUsage.ModelContextWindow)
	}
	if got, want := event.TokenUsage.Last.TotalTokens, int64(5750); got != want {
		t.Fatalf("last.totalTokens=%d, want %d", got, want)
	}
}

func TestTurnStreamCriticalEventSurvivesBackpressure(t *testing.T) {
	t.Parallel()

	stream := newTurnStream("turn-1", slog.New(slog.NewJSONHandler(io.Discard, nil)))
	for i := 0; i < turnStreamPendingLimit+turnStreamBufferSize+128; i++ {
		stream.enqueue(TurnEvent{
			Type:  TurnEventTypeReasoningDelta,
			Delta: "chunk",
		}, false)
	}

	stream.enqueue(TurnEvent{
		Type:     TurnEventTypeApprovalRequired,
		Approval: ApprovalRequest{ApprovalID: "approval-1"},
	}, false)

	seenApproval := false
	timeout := time.After(3 * time.Second)
	for !seenApproval {
		select {
		case event, ok := <-stream.events():
			if !ok {
				t.Fatalf("turn stream closed before approval_required was delivered")
			}
			if event.Type == TurnEventTypeApprovalRequired {
				seenApproval = true
			}
		case <-timeout:
			t.Fatalf("timed out waiting for queued events")
		}
	}

	if !seenApproval {
		t.Fatalf("approval_required event was dropped under backpressure")
	}
}

func TestTurnStreamCoalescesHighFrequencyDeltas(t *testing.T) {
	t.Parallel()

	stream := newTurnStream("turn-1", slog.New(slog.NewJSONHandler(io.Discard, nil)))
	stream.enqueue(TurnEvent{
		Type:   TurnEventTypeAgentMessageDelta,
		ItemID: "item-1",
		Delta:  "hello",
	}, false)
	stream.enqueue(TurnEvent{
		Type:   TurnEventTypeAgentMessageDelta,
		ItemID: "item-1",
		Delta:  " world",
	}, false)
	stream.enqueue(TurnEvent{
		Type:       TurnEventTypeCompleted,
		StopReason: "end_turn",
	}, true)

	first := readTurnEventWithTimeout(t, stream.events())
	if first.Type != TurnEventTypeAgentMessageDelta {
		t.Fatalf("first event type=%q, want %q", first.Type, TurnEventTypeAgentMessageDelta)
	}
	if first.Delta != "hello world" {
		t.Fatalf("delta=%q, want %q", first.Delta, "hello world")
	}

	second := readTurnEventWithTimeout(t, stream.events())
	if second.Type != TurnEventTypeCompleted {
		t.Fatalf("second event type=%q, want %q", second.Type, TurnEventTypeCompleted)
	}
}

func readTurnEventWithTimeout(t *testing.T, ch <-chan TurnEvent) TurnEvent {
	t.Helper()

	select {
	case event, ok := <-ch:
		if !ok {
			t.Fatalf("turn stream closed unexpectedly")
		}
		return event
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for turn event")
		return TurnEvent{}
	}
}

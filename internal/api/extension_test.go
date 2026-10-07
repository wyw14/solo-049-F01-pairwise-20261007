package api

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	passsvc "orbitlink/internal/pass"
	"orbitlink/internal/receiver"
	"orbitlink/internal/rf"
)

func beginAcquisition(t *testing.T, runtime *Runtime, passID string) {
	t.Helper()
	attempt, err := runtime.Attempts.Begin(passID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(passID, attempt.ID); err != nil {
		t.Fatal(err)
	}
}

func TestExtendSynchronizesAllResources(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	beginAcquisition(t, runtime, value.ID)
	attempt, _ := runtime.Attempts.Current(value.ID)
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	if _, err := runtime.Passes.AcceptDiversity(value.ID, results); err != nil {
		t.Fatal(err)
	}
	if _, found := runtime.Recorder.ActiveForPass(value.ID); !found {
		t.Fatal("expected an active recording segment")
	}

	result, err := runtime.Extensions.Extend(value.ID, 2*time.Minute, "sync-1")
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.Replay {
		t.Fatalf("extension not applied: %#v", result)
	}
	if len(result.Items) != 6 {
		t.Fatalf("expected six item results, got %#v", result.Items)
	}
	for _, item := range result.Items {
		if item.Status != passsvc.ExtensionApplied {
			t.Fatalf("item %s was %s: %s", item.Name, item.Status, item.Detail)
		}
	}

	newLOS := value.Window.LOS.Add(2 * time.Minute)
	updated, _ := runtime.Passes.Get(value.ID)
	if !updated.Window.LOS.Equal(newLOS) {
		t.Fatalf("pass LOS = %s, want %s", updated.Window.LOS, newLOS)
	}
	var reservationEnd time.Time
	for _, interval := range runtime.Timeline.List(value.AntennaID) {
		if interval.ReservationID == value.ReservationID {
			reservationEnd = interval.End
		}
	}
	if !reservationEnd.Equal(newLOS) {
		t.Fatalf("reservation end = %s, want %s", reservationEnd, newLOS)
	}
	deadline, found := runtime.Deadlines.Get(value.ID)
	if !found || !deadline.CivilLOS.Equal(newLOS) {
		t.Fatalf("deadline not synced: %#v", deadline)
	}
	var lastExecuteAt time.Time
	retired := 0
	for _, command := range runtime.Commands.List(value.ID) {
		if command.Retired {
			retired++
			continue
		}
		if command.ExecuteAt.After(lastExecuteAt) {
			lastExecuteAt = command.ExecuteAt
		}
	}
	if retired != 0 {
		t.Fatalf("extension retired %d pointing commands", retired)
	}
	if !lastExecuteAt.Equal(newLOS) {
		t.Fatalf("pointing queue ends at %s, want %s", lastExecuteAt, newLOS)
	}
	for _, chainID := range []string{"RF-01", "RF-02"} {
		state, _ := runtime.RF.State(chainID)
		if state.PassID != value.ID {
			t.Fatalf("chain %s planned for %q", chainID, state.PassID)
		}
	}
	segment, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found || !segment.ExpectedLOS.Equal(newLOS) {
		t.Fatalf("recording expectation not synced: %#v", segment)
	}
	window, found := runtime.Windows.Get(value.ID)
	if !found || !window.LOS.Equal(newLOS) {
		t.Fatalf("window store not synced: %#v", window)
	}
}

func TestExtendConflictReportsItemsAndAppliesNothing(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]
	second, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: first.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: first.Window.LOS.Add(time.Minute), LOS: first.Window.LOS.Add(9 * time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(second.ID, second.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID); err != nil {
		t.Fatal(err)
	}
	beginAcquisition(t, runtime, first.ID)

	result, err := runtime.Extensions.Extend(first.ID, 5*time.Minute, "")
	var conflict *passsvc.ExtensionConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected extension conflict, got %v", err)
	}
	if result.Applied {
		t.Fatal("conflicting extension was applied")
	}
	failed := 0
	for _, item := range result.Items {
		switch item.Status {
		case passsvc.ExtensionFailed:
			failed++
			if item.Name != passsvc.ExtensionItemReservation {
				t.Fatalf("unexpected failed item %s: %s", item.Name, item.Detail)
			}
		case passsvc.ExtensionNotApplied:
		default:
			t.Fatalf("item %s unexpectedly %s", item.Name, item.Status)
		}
	}
	if failed != 1 {
		t.Fatalf("expected exactly one failed item, got %d", failed)
	}
	updated, _ := runtime.Passes.Get(first.ID)
	if !updated.Window.LOS.Equal(first.Window.LOS) {
		t.Fatalf("pass window moved despite conflict: %s", updated.Window.LOS)
	}
	for _, interval := range runtime.Timeline.List(first.AntennaID) {
		if interval.ReservationID == first.ReservationID && !interval.End.Equal(first.Window.LOS) {
			t.Fatalf("reservation moved despite conflict: %s", interval.End)
		}
	}
	deadline, _ := runtime.Deadlines.Get(first.ID)
	if !deadline.CivilLOS.Equal(first.Window.LOS) {
		t.Fatalf("deadline moved despite conflict: %s", deadline.CivilLOS)
	}
}

func TestExtendReceiveChainConflict(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	if _, err := runtime.RF.Apply(rf.Plan{PassID: "other-pass", ChainID: "RF-01", CenterHz: 2_245_000_000, Polarization: rf.RightCircular}); err != nil {
		t.Fatal(err)
	}
	beginAcquisition(t, runtime, value.ID)

	result, err := runtime.Extensions.Extend(value.ID, time.Minute, "")
	var conflict *passsvc.ExtensionConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("expected receive chain conflict, got %v", err)
	}
	for _, item := range result.Items {
		if item.Name == passsvc.ExtensionItemReceiveChainPlan && item.Status != passsvc.ExtensionFailed {
			t.Fatalf("receive chain plan item = %s", item.Status)
		}
		if item.Name == passsvc.ExtensionItemReservation && item.Status != passsvc.ExtensionNotApplied {
			t.Fatalf("reservation item = %s", item.Status)
		}
	}
	updated, _ := runtime.Passes.Get(value.ID)
	if !updated.Window.LOS.Equal(value.Window.LOS) {
		t.Fatal("window moved despite receive chain conflict")
	}
}

func TestExtendIsIdempotent(t *testing.T) {
	runtime := newTestRuntime(t)
	value := runtime.Passes.List()[0]
	beginAcquisition(t, runtime, value.ID)

	first, err := runtime.Extensions.Extend(value.ID, 90*time.Second, "key-a")
	if err != nil || !first.Applied {
		t.Fatalf("first extension: %#v %v", first, err)
	}
	replay, err := runtime.Extensions.Extend(value.ID, 90*time.Second, "key-a")
	if err != nil || !replay.Replay || !replay.Applied {
		t.Fatalf("expected idempotent replay, got %#v %v", replay, err)
	}
	after, _ := runtime.Passes.Get(value.ID)
	if !after.Window.LOS.Equal(value.Window.LOS.Add(90 * time.Second)) {
		t.Fatalf("idempotent replay moved the window: %s", after.Window.LOS)
	}
	if _, err := runtime.Extensions.Extend(value.ID, time.Minute, "key-a"); err == nil {
		t.Fatal("reused idempotency key with a different duration")
	}
	third, err := runtime.Extensions.Extend(value.ID, time.Minute, "")
	if err != nil || !third.Applied || third.Replay {
		t.Fatalf("keyless extension: %#v %v", third, err)
	}
	fourth, err := runtime.Extensions.Extend(value.ID, time.Minute, "")
	if err != nil || !fourth.Replay {
		t.Fatalf("keyless replay: %#v %v", fourth, err)
	}
	final, _ := runtime.Passes.Get(value.ID)
	want := value.Window.LOS.Add(90*time.Second + time.Minute)
	if !final.Window.LOS.Equal(want) {
		t.Fatalf("final LOS = %s, want %s", final.Window.LOS, want)
	}
}

func TestExtendIdempotencySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	runtime, err := NewRuntime(dir)
	if err != nil {
		t.Fatal(err)
	}
	value := runtime.Passes.List()[0]
	beginAcquisition(t, runtime, value.ID)
	if _, err := runtime.Extensions.Extend(value.ID, time.Minute, "durable"); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewRuntime(dir)
	if err != nil {
		t.Fatal(err)
	}
	result, err := restarted.Extensions.Extend(value.ID, time.Minute, "durable")
	if err != nil || !result.Replay {
		t.Fatalf("expected replay after restart, got %#v %v", result, err)
	}
	after, _ := restarted.Passes.Get(value.ID)
	if !after.Window.LOS.Equal(value.Window.LOS.Add(time.Minute)) {
		t.Fatalf("restart replay moved the window: %s", after.Window.LOS)
	}
}

func TestExtendConflictHTTPReport(t *testing.T) {
	runtime := newTestRuntime(t)
	first := runtime.Passes.List()[0]
	second, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: first.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: first.Window.LOS.Add(time.Minute), LOS: first.Window.LOS.Add(9 * time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(second.ID, second.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(second.ID, second.AntennaID); err != nil {
		t.Fatal(err)
	}
	beginAcquisition(t, runtime, first.ID)

	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()
	response := requestJSON(t, server.Client(), "POST", server.URL+"/api/passes/"+first.ID+"/extend", `{"seconds":300,"idempotency_key":"http-1"}`)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d", response.StatusCode)
	}
	var body struct {
		Applied bool `json:"applied"`
		Items   []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"items"`
	}
	decodeBody(t, response, &body)
	if body.Applied || len(body.Items) != 6 {
		t.Fatalf("unexpected conflict body: %#v", body)
	}
	statuses := map[string]string{}
	for _, item := range body.Items {
		statuses[item.Name] = item.Status
	}
	if statuses[passsvc.ExtensionItemReservation] != string(passsvc.ExtensionFailed) {
		t.Fatalf("reservation item = %q", statuses[passsvc.ExtensionItemReservation])
	}
	if statuses[passsvc.ExtensionItemRecording] != string(passsvc.ExtensionNotApplied) {
		t.Fatalf("recording item = %q", statuses[passsvc.ExtensionItemRecording])
	}
}

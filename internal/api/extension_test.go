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

func TestExtensionSynchronizesStationResources(t *testing.T) {
	runtime := newTestRuntime(t)
	value := trackPass(t, runtime)
	oldLOS := value.Window.LOS
	newLOS := oldLOS.Add(2 * time.Minute)

	result, err := runtime.Extensions.Extend(value.ID, "req-extend-1", 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Applied || result.Duplicate {
		t.Fatalf("extension not applied: %#v", result)
	}
	if !result.NewLOS.Equal(newLOS) {
		t.Fatalf("new LOS %s, want %s", result.NewLOS, newLOS)
	}
	for _, item := range result.Items {
		if item.Status != passsvc.ExtensionApplied {
			t.Fatalf("item %s not applied: %#v", item.Item, item)
		}
	}

	interval, err := runtime.Timeline.CheckExtend(value.ReservationID, newLOS)
	if err != nil || !interval.End.Equal(newLOS) {
		t.Fatalf("reservation end %s: %v", interval.End, err)
	}
	updated, _ := runtime.Passes.Get(value.ID)
	if !updated.Window.LOS.Equal(newLOS) {
		t.Fatalf("pass window LOS %s, want %s", updated.Window.LOS, newLOS)
	}
	deadline, found := runtime.Deadlines.Get(value.ID)
	if !found || !deadline.CivilLOS.Equal(newLOS) {
		t.Fatalf("deadline civil LOS %s", deadline.CivilLOS)
	}
	if deadline.MonotonicLOS.IsZero() {
		t.Fatal("monotonic deadline was not kept active")
	}
	if remaining, err := runtime.Deadlines.Remaining(value.ID); err != nil || remaining <= 0 {
		t.Fatalf("monotonic deadline remaining %s: %v", remaining, err)
	}
	pointingAtLOS := false
	for _, command := range runtime.Commands.List(value.ID) {
		if !command.Retired && command.ExecuteAt.Equal(newLOS) {
			pointingAtLOS = true
		}
		if !command.Retired && command.ExecuteAt.After(newLOS) {
			t.Fatalf("pointing command beyond new LOS: %#v", command)
		}
	}
	if !pointingAtLOS {
		t.Fatal("pointing queue has no command at the extended LOS")
	}
	for _, chainID := range value.RequiredReceiverChains {
		state, found := runtime.RF.State(chainID)
		if !found || !state.PlanUntil.Equal(newLOS) {
			t.Fatalf("receiver chain %s plan until %s", chainID, state.PlanUntil)
		}
	}
	segment, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found || !segment.ExpectedUntil.Equal(newLOS) {
		t.Fatalf("recording expectation %s", segment.ExpectedUntil)
	}
}

func TestExtensionConflictReportsItemsAndAppliesNothing(t *testing.T) {
	runtime := newTestRuntime(t)
	value := trackPass(t, runtime)
	neighbor, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: value.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: value.Window.LOS.Add(time.Minute), LOS: value.Window.LOS.Add(9 * time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(neighbor.ID, neighbor.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(neighbor.ID, neighbor.AntennaID); err != nil {
		t.Fatal(err)
	}

	result, err := runtime.Extensions.Extend(value.ID, "req-conflict-1", 2*time.Minute)
	if !errors.Is(err, passsvc.ErrExtensionConflict) {
		t.Fatalf("expected extension conflict, got %v", err)
	}
	if result.Applied {
		t.Fatal("conflicted extension must not be applied")
	}
	items := make(map[string]passsvc.ExtensionItem, len(result.Items))
	for _, item := range result.Items {
		items[item.Item] = item
	}
	if len(items) != 6 {
		t.Fatalf("expected six itemized results, got %#v", result.Items)
	}
	if items[passsvc.ExtensionItemReservation].Status != passsvc.ExtensionConflict {
		t.Fatalf("reservation item: %#v", items[passsvc.ExtensionItemReservation])
	}
	for _, name := range []string{passsvc.ExtensionItemWindow, passsvc.ExtensionItemDeadline, passsvc.ExtensionItemPointing, passsvc.ExtensionItemReceiverPlan, passsvc.ExtensionItemRecording} {
		if items[name].Status != passsvc.ExtensionSkipped {
			t.Fatalf("item %s should be skipped: %#v", name, items[name])
		}
	}

	after, _ := runtime.Passes.Get(value.ID)
	if !after.Window.LOS.Equal(value.Window.LOS) {
		t.Fatalf("window moved to %s despite conflict", after.Window.LOS)
	}
	interval, err := runtime.Timeline.CheckExtend(value.ReservationID, value.Window.LOS)
	if err != nil || !interval.End.Equal(value.Window.LOS) {
		t.Fatalf("reservation end moved to %s", interval.End)
	}
	deadline, _ := runtime.Deadlines.Get(value.ID)
	if !deadline.CivilLOS.Equal(value.Window.LOS) {
		t.Fatalf("deadline moved to %s despite conflict", deadline.CivilLOS)
	}
	for _, command := range runtime.Commands.List(value.ID) {
		if !command.Retired && command.ExecuteAt.After(value.Window.LOS) {
			t.Fatalf("pointing command queued despite conflict: %#v", command)
		}
	}
	if state, _ := runtime.RF.State("RF-01"); !state.PlanUntil.IsZero() {
		t.Fatalf("receiver plan horizon moved to %s despite conflict", state.PlanUntil)
	}
	segment, found := runtime.Recorder.ActiveForPass(value.ID)
	if !found || !segment.ExpectedUntil.IsZero() {
		t.Fatalf("recording expectation moved to %s despite conflict", segment.ExpectedUntil)
	}
}

func TestExtensionIsIdempotentPerRequest(t *testing.T) {
	runtime := newTestRuntime(t)
	value := trackPass(t, runtime)

	first, err := runtime.Extensions.Extend(value.ID, "req-idem-1", 90*time.Second)
	if err != nil || !first.Applied {
		t.Fatalf("first extension: %#v %v", first, err)
	}
	second, err := runtime.Extensions.Extend(value.ID, "req-idem-1", 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || !second.Applied {
		t.Fatalf("replayed request must be a duplicate: %#v", second)
	}
	if !second.NewLOS.Equal(first.NewLOS) {
		t.Fatalf("replayed request moved LOS to %s", second.NewLOS)
	}
	after, _ := runtime.Passes.Get(value.ID)
	if !after.Window.LOS.Equal(value.Window.LOS.Add(90 * time.Second)) {
		t.Fatalf("replayed request extended twice, LOS %s", after.Window.LOS)
	}
	extensionCommands := 0
	for _, command := range runtime.Commands.List(value.ID) {
		if !command.Retired && command.ExecuteAt.After(value.Window.LOS) {
			extensionCommands++
		}
	}
	if extensionCommands != 2 {
		t.Fatalf("replayed request duplicated pointing commands: %d", extensionCommands)
	}
	if _, err := runtime.Extensions.Extend(value.ID, "req-idem-1", time.Minute); err == nil {
		t.Fatal("request id reuse with a different duration must fail")
	}
	third, err := runtime.Extensions.Extend(value.ID, "req-idem-2", 90*time.Second)
	if err != nil || third.Duplicate {
		t.Fatalf("new request id must extend again: %#v %v", third, err)
	}
	final, _ := runtime.Passes.Get(value.ID)
	if !final.Window.LOS.Equal(value.Window.LOS.Add(3 * time.Minute)) {
		t.Fatalf("final LOS %s", final.Window.LOS)
	}
}

func TestExtendEndpointReturnsItemizedResults(t *testing.T) {
	runtime := newTestRuntime(t)
	value := trackPass(t, runtime)
	router, err := NewRouter(runtime)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(router)
	defer server.Close()

	response := requestJSON(t, server.Client(), "POST", server.URL+"/api/passes/"+value.ID+"/extend", `{"seconds": 60, "request_id": "req-http-1"}`)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("extend returned %d", response.StatusCode)
	}
	var result passsvc.ExtensionResult
	decodeBody(t, response, &result)
	if !result.Applied || result.RequestID != "req-http-1" || len(result.Items) != 6 {
		t.Fatalf("unexpected extension result: %#v", result)
	}

	response = requestJSON(t, server.Client(), "POST", server.URL+"/api/passes/"+value.ID+"/extend", `{"seconds": 60, "request_id": "req-http-1"}`)
	var replay passsvc.ExtensionResult
	decodeBody(t, response, &replay)
	if !replay.Duplicate {
		t.Fatalf("replayed request must be a duplicate: %#v", replay)
	}

	neighbor, err := runtime.Passes.Create(passsvc.CreateRequest{SatelliteID: "SAT-C07", AntennaID: value.AntennaID, RFChains: []string{"RF-03"}, RequiredReceiverChains: []string{"RF-03"}, AOS: result.NewLOS.Add(time.Minute), LOS: result.NewLOS.Add(9 * time.Minute), Revision: 1, OwnerStation: "north"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.WindowRecompute.Apply(neighbor.ID, neighbor.Window); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Reservations.ReservePass(neighbor.ID, neighbor.AntennaID); err != nil {
		t.Fatal(err)
	}
	response = requestJSON(t, server.Client(), "POST", server.URL+"/api/passes/"+value.ID+"/extend", `{"seconds": 300, "request_id": "req-http-2"}`)
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("conflicted extend returned %d", response.StatusCode)
	}
	var conflict passsvc.ExtensionResult
	decodeBody(t, response, &conflict)
	if conflict.Applied {
		t.Fatal("conflicted extension must not be applied")
	}
}

func trackPass(t *testing.T, runtime *Runtime) passsvc.Pass {
	t.Helper()
	value := runtime.Passes.List()[0]
	for _, chainID := range value.RequiredReceiverChains {
		if _, err := runtime.RF.Apply(rf.Plan{PassID: value.ID, ChainID: chainID, CenterHz: 2_245_000_000, DopplerHz: 12_000, Polarization: rf.RightCircular}); err != nil {
			t.Fatal(err)
		}
	}
	attempt, err := runtime.Attempts.Begin(value.ID, "RF-01")
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Passes.BeginAcquisition(value.ID, attempt.ID); err != nil {
		t.Fatal(err)
	}
	results := []receiver.AcquisitionResult{
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-01", Locked: true, Synced: true, Epoch: 7},
		{PassID: value.ID, AttemptID: attempt.ID, ChainID: "RF-02", Locked: true, Synced: true, Epoch: 7},
	}
	if _, err := runtime.Passes.AcceptDiversity(value.ID, results); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Deadlines.Activate(value.ID, time.Until(value.Window.LOS)); err != nil {
		t.Fatal(err)
	}
	value, _ = runtime.Passes.Get(value.ID)
	return value
}

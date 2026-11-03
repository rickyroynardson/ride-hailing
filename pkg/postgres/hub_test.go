package postgres

import "testing"

func TestHubSignalsOnlyItsKeyAndFolds(t *testing.T) {
	h := NewHub(nil, "test")
	a, stopA := h.Subscribe("a")
	b, stopB := h.Subscribe("b")
	defer stopB()

	h.signal("a")
	h.signal("a") // folds into the pending one
	select {
	case <-a:
	default:
		t.Fatal("a not signalled")
	}
	select {
	case <-a:
		t.Fatal("two signals didn't fold into one")
	default:
	}
	select {
	case <-b:
		t.Fatal("b signalled for a's change")
	default:
	}

	h.signalAll()
	if len(b) != 1 || len(a) != 1 {
		t.Fatal("signalAll missed a subscriber")
	}
	stopA()
	if _, ok := h.subs["a"]; ok {
		t.Fatal("stopping the last subscriber left its key behind")
	}
}

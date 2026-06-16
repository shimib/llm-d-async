package plugins

import (
	"context"
	"encoding/json"
	"testing"
)

type stubPlugin struct {
	name string
}

func (s *stubPlugin) TypedName() TypedName { return TypedName{Type: "stub", Name: s.name} }

func TestTypedName_String(t *testing.T) {
	tn := TypedName{Type: "multipart", Name: "whisper"}
	if got, want := tn.String(), "whisper/multipart"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestRegistry_RegisterAndLookup(t *testing.T) {
	// Use a unique type so we don't collide with other tests/packages.
	const typ = "plugins_test.stub"
	called := false
	Register(typ, func(name string, _ json.RawMessage, _ Handle) (Plugin, error) {
		called = true
		return &stubPlugin{name: name}, nil
	})
	t.Cleanup(func() { delete(Registry, typ) })

	factory, ok := Registry[typ]
	if !ok {
		t.Fatalf("type %q not found in Registry after Register", typ)
	}
	p, err := factory("inst", nil, NewHandle(context.Background()))
	if err != nil {
		t.Fatalf("factory returned error: %v", err)
	}
	if !called {
		t.Error("factory was not invoked")
	}
	if p.TypedName().Name != "inst" {
		t.Errorf("plugin name = %q, want %q", p.TypedName().Name, "inst")
	}
}

func TestHandle_AddAndGet(t *testing.T) {
	h := NewHandle(context.Background())
	if h.Context() == nil {
		t.Error("Context() returned nil")
	}
	if h.Plugin("missing") != nil {
		t.Error("Plugin(missing) should be nil")
	}

	p := &stubPlugin{name: "a"}
	h.AddPlugin("a", p)
	if h.Plugin("a") != p {
		t.Error("Plugin(a) did not return the added plugin")
	}
	if len(h.GetAllPlugins()) != 1 {
		t.Errorf("GetAllPlugins len = %d, want 1", len(h.GetAllPlugins()))
	}
	if _, ok := h.GetAllPluginsWithNames()["a"]; !ok {
		t.Error("GetAllPluginsWithNames missing key 'a'")
	}
}

func TestPluginByType(t *testing.T) {
	h := NewHandle(context.Background())
	h.AddPlugin("a", &stubPlugin{name: "a"})

	got, err := PluginByType[*stubPlugin](h, "a")
	if err != nil {
		t.Fatalf("PluginByType error: %v", err)
	}
	if got.name != "a" {
		t.Errorf("name = %q, want %q", got.name, "a")
	}

	if _, err := PluginByType[*stubPlugin](h, "missing"); err == nil {
		t.Error("expected error for missing plugin")
	}
}

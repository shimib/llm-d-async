package plugins

import (
	"context"
	"fmt"
)

// Handle provides plugins a set of standard data and tools to work with. It is a
// trimmed, Kubernetes-free analog of EPP's Handle: it carries a context and the
// set of instantiated plugins, but omits EPP's pod-listing concerns.
type Handle interface {
	// Context returns a context the plugins can use, if they need one.
	Context() context.Context

	HandlePlugins
}

// HandlePlugins defines a set of APIs to work with instantiated plugins.
type HandlePlugins interface {
	// Plugin returns the named plugin instance, or nil if none is registered.
	Plugin(name string) Plugin

	// AddPlugin adds a plugin to the set of known plugin instances.
	AddPlugin(name string, plugin Plugin)

	// GetAllPlugins returns all of the known plugins.
	GetAllPlugins() []Plugin

	// GetAllPluginsWithNames returns all of the known plugins keyed by name.
	GetAllPluginsWithNames() map[string]Plugin
}

// handle is the default Handle implementation.
type handle struct {
	ctx context.Context
	HandlePlugins
}

// Context returns a context the plugins can use, if they need one.
func (h *handle) Context() context.Context {
	return h.ctx
}

// handlePlugins implements the set of APIs to work with instantiated plugins.
type handlePlugins struct {
	plugins map[string]Plugin
}

// Plugin returns the named plugin instance.
func (h *handlePlugins) Plugin(name string) Plugin {
	return h.plugins[name]
}

// AddPlugin adds a plugin to the set of known plugin instances.
func (h *handlePlugins) AddPlugin(name string, plugin Plugin) {
	h.plugins[name] = plugin
}

// GetAllPlugins returns all of the known plugins.
func (h *handlePlugins) GetAllPlugins() []Plugin {
	result := make([]Plugin, 0, len(h.plugins))
	for _, plugin := range h.plugins {
		result = append(result, plugin)
	}
	return result
}

// GetAllPluginsWithNames returns all of the known plugins keyed by name.
func (h *handlePlugins) GetAllPluginsWithNames() map[string]Plugin {
	return h.plugins
}

// NewHandle returns a Handle backed by the given context and an empty plugin set.
func NewHandle(ctx context.Context) Handle {
	return &handle{
		ctx:           ctx,
		HandlePlugins: &handlePlugins{plugins: map[string]Plugin{}},
	}
}

// PluginByType retrieves the named plugin and asserts it implements P.
func PluginByType[P Plugin](handlePlugins HandlePlugins, name string) (P, error) {
	var zero P

	rawPlugin := handlePlugins.Plugin(name)
	if rawPlugin == nil {
		return zero, fmt.Errorf("there is no plugin with the name '%s' defined", name)
	}
	plugin, ok := rawPlugin.(P)
	if !ok {
		return zero, fmt.Errorf("the plugin with the name '%s' is not an instance of %T", name, zero)
	}
	return plugin, nil
}
